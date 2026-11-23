#!/usr/bin/env python3
"""Run two Go routers against a private etcd and two fixture workers."""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import random
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
ETCD = "quay.io/coreos/etcd:v3.6.5@sha256:3397341272b9e0a6f44d7e3fc7c321c6efe6cbe82ce866b9b01d0c704bfc5bf3"
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def ports(count):
    with ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(count)]
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [f"127.0.0.1:{sock.getsockname()[1]}" for sock in sockets]


def request(url, payload=None, token=None):
    body = None if payload is None else json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    try:
        res = HTTP.open(urllib.request.Request(url, data=body, headers=headers), timeout=5)
    except urllib.error.HTTPError as error:
        res = error
    with res:
        data = res.read().decode()
        return res.status, data


def eventually(check, timeout=15):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            last = check()
            if last:
                return last
        except (OSError, urllib.error.URLError) as exc:
            last = exc
        time.sleep(0.1)
    raise AssertionError(f"condition did not hold in {timeout}s: {last}")


def expect(response, status=200, code=None):
    number, data = response
    if number != status:
        raise AssertionError(f"expected HTTP {status}, got {number}: {data}")
    obj = json.loads(data) if data else {}
    if code and obj.get("code") != code:
        raise AssertionError(f"expected {code}, got {obj}")
    return obj


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def run(args, tmp, logs):
    token, private = secrets.token_urlsafe(), secrets.token_urlsafe()
    rng = random.Random(args.seed)
    def version(*command):
        return subprocess.check_output(command, text=True).strip()
    metadata = {"policy":args.policy, "seed":args.seed, "etcd_image":ETCD,
                "go":version("go","version"), "docker":version("docker","version","--format","{{.Server.Version}}"),
                "git":version("git","-C",str(ROOT),"rev-parse","HEAD")}
    (logs/"run.json").write_text(json.dumps(metadata,indent=2)+"\n")
    env = dict(os.environ, DURO_TOKEN=token, DURO_CONTROLLER_TOKEN=private)
    pg, ps, wa, wb, admin, r1, r2, p1, p2 = ports(9)
    routers, priv = ["http://" + x for x in (r1, r2)], ["http://" + x for x in (p1, p2)]
    for name, path in [("router","./cmd/router"),("mock","./cmd/mock"),("probe","./e2e/probe")]:
        subprocess.run(["go", "build", "-o", tmp / name, path], cwd=ROOT, check=True)
    events = []

    def mark(name, **values):
        events.append({"scenario": name, **values})
        (logs / "scenarios.json").write_text(json.dumps(events, indent=2) + "\n")

    def api(idx, op, obj):
        return request(routers[idx] + "/v1/" + op, obj, token)

    def owner():
        result = request("http://" + admin + "/owner?object_id=q")
        return json.loads(result[1]) if result[0] == 200 else None

    def fault(kind, count=1, worker_id=""):
        with (logs/"faults.jsonl").open("a") as journal:
            print(json.dumps(dict(kind=kind, count=count, worker_id=worker_id, time=time.time())),file=journal,flush=True)
        status, body = request("http://" + admin + "/fault", dict(kind=kind, count=count, worker_id=worker_id))
        if status not in (200, 204):
            raise AssertionError(f"fault setup failed: {status} {body}")

    with ExitStack() as resources:
        def launch(name, command):
            log = resources.enter_context((logs / (name + ".log")).open("w"))
            proc = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
            resources.callback(stop, proc)
            return proc

        etcd_name = "duro-e2e-" + uuid.uuid4().hex
        subprocess.run(["docker", "run", "--detach", "--name", etcd_name, "-p", pg + ":2379", ETCD,
                        "/usr/local/bin/etcd", "--name=default", "--data-dir=/tmp/duro-etcd",
                        "--listen-client-urls=http://0.0.0.0:2379",
                        "--advertise-client-urls=http://127.0.0.1:2379"], check=True, stdout=subprocess.DEVNULL)
        resources.callback(subprocess.run, ["docker", "rm", "-f", etcd_name], stdout=subprocess.DEVNULL)
        eventually(lambda: request("http://" + pg + "/health")[0] == 200)
        launch("mock", [tmp/"mock", "-state-listen", ps, "-workers", "a="+wa+",b="+wb,
                        "-admin-listen", admin, "-etcd", pg, "-lease", "9s"])
        processes = []
        for idx, (addr, peer) in enumerate([(r1, p1), (r2, p2)]):
            processes.append(launch("router" + str(idx), [tmp/"router", "-listen", addr, "-private-listen", peer,
                                                          "-etcd", pg, "-state", ps, "-policy", args.policy]))
        for addr in routers:
            eventually(lambda: request(addr + "/readyz")[0] == 204)
        def leader():
            for i, addr in enumerate(priv):
                if processes[i].poll() is None:
                    _, metrics = request(addr + "/metrics")
                    if f'duro_routing_is_controller{{policy="{args.policy}"}} 1' in metrics:
                        return i
            return None
        eventually(lambda: leader() is not None)
        elected = leader()
        follower = 1 - elected
        expect(request(routers[0] + "/v1/read", {"object_id":"q"}), 401, "UNAUTHORIZED")
        for name, methods in [("quota", ["spend","getRemaining"]), ("auction", ["bid","getSummary","close"])]:
            bundle = (ROOT/"e2e"/"fixtures"/(name+".js")).read_bytes()
            packet = {"manifest":{"type":name,"version":"1","runtime":"javascript-v8","format":"esm",
                                  "methods":methods,"sha256":hashlib.sha256(bundle).hexdigest()},
                      "bundle":base64.b64encode(bundle).decode()}
            eventually(lambda: api(0,"registerType",packet)[0] == 200)
            expect(api(1,"registerType",packet))
        create = dict(object_id="q", type="quota", version="1", input={"limit":10}, request_id="new-q")
        indices=[follower,elected]
        rng.shuffle(indices)
        with ThreadPoolExecutor(2) as pool:
            created=list(pool.map(lambda idx:expect(api(idx,"create",create)),indices))
        assert created[0]==created[1]
        assert owner()["epoch"]==1
        expect(api(0,"create",dict(create, request_id="different")),409,"OBJECT_ALREADY_EXISTS")
        for wid in ["a","b"]:
            fault("worker-down",100000,wid)
        assert expect(api(1,"describe",{"object_id":"q"}))["version"] == "1"
        assert expect(api(0,"read",{"object_id":"q"}))["kv"] == {"remaining":10}
        for wid in ["a","b"]:
            fault("worker-down",0,wid)
        mark("concurrent create, replay, describe and primary read with both workers down")

        def spend(i, amount=1):
            return dict(object_id="q",method="spend",argument={"amount":amount},request_id=i)
        with ThreadPoolExecutor(2) as pool:
            replies=list(pool.map(lambda idx: expect(api(idx,"invoke",spend("twice"))), [0,1]))
        assert all(v["result"]["remaining"]==9 for v in replies)
        expect(api(1,"invoke",spend("twice",2)),409,"REQUEST_ID_CONFLICT")
        for idx in [0,1]:
            expect(api(idx,"invoke",spend("reject",100)),422,"DOMAIN_REJECTED")
        mark("concurrent duplicate, conflict and durable rejection")

        for kind, expected in [("precommit",8),("postcommit",7)]:
            fault(kind)
            expect(api(0,"invoke",spend(kind)),503,"UNAVAILABLE")
            assert expect(api(1,"invoke",spend(kind)))["result"]["remaining"] == expected
            assert expect(api(0,"read",{"object_id":"q"}))["kv"]["remaining"] == expected
        fault("overloaded")
        expect(api(1,"invoke",spend("load")),503,"OVERLOADED")
        fault("execution-timeout")
        expect(api(1,"invoke",spend("slow")),504,"EXECUTION_TIMEOUT")
        mark("failure before commit, lost reply after commit, overload and timeout")

        before = owner()
        target = "b" if before["worker_id"]=="a" else "a"
        def transfer():
            request(priv[elected]+"/admin/move",{"object_id":"q","target":target},private)
            current = owner()
            return current and current["worker_id"]==target and current["epoch"]>before["epoch"]
        eventually(transfer)
        assert expect(api(1,"invoke",spend("after-move")))["result"]["remaining"]==6
        after_move = owner()
        mark("manual transfer and stale cache", old=before, new=after_move)

        auction=dict(object_id="a1",type="auction",version="1",request_id="new-a",input={"startPrice":0})
        expect(api(follower,"create",auction))
        bid=dict(object_id="a1",method="bid",argument={"amount":3,"bidder":"A"},request_id="bid")
        assert expect(api(elected,"invoke",bid))["result"]["price"]==3
        expect(api(elected,"invoke",dict(bid,method="close",argument=None,request_id="close")))
        expect(api(follower,"invoke",dict(bid,request_id="late",argument={"amount":9,"bidder":"B"})),422,"DOMAIN_REJECTED")
        assert expect(api(follower,"read",{"object_id":"a1"}))["kv"]["auction"]=={"price":3,"bidder":"A","closed":True}
        mark("auction invariants")

        processes[elected].kill()
        processes[elected].wait()
        eventually(lambda:leader()==follower,timeout=18)
        records=[json.loads(line) for line in (logs/f"router{elected}.log").read_text().splitlines()]
        old_term=next(row["term"] for row in records if row.get("msg")=="controller elected")
        subprocess.run([tmp/"probe","-address",ps,"-old-term",str(old_term)],check=True)
        started=time.monotonic()
        fault("worker-down",100000,target)
        def recovered():
            after=owner()
            if not after or after["worker_id"]==target or after["epoch"]<=before["epoch"]+1:
                return False
            code,body=api(follower,"invoke",spend("after-failure"))
            return code==200 and json.loads(body)["result"]["remaining"]==5
        eventually(recovered,timeout=30)
        elapsed=time.monotonic()-started
        fault("worker-down",0,target)
        subprocess.run([tmp/"probe","-address", {"a": wa, "b": wb}[target],
                        "-worker-id",target,"-epoch",str(after_move["epoch"])],check=True)
        assert expect(api(follower,"invoke",spend("after-failure")))["result"]["remaining"]==5
        assert expect(api(follower,"read",{"object_id":"q"}))["kv"]["remaining"]==5
        mark("leader loss, expired owner recovery and replay", recovery_seconds=round(elapsed,3), owner=owner())
        _, metrics=request(priv[follower]+"/metrics")
        assert "duro_routing_request_seconds_count" in metrics
        for secret in (token,private):
            for name in ["router0.log","router1.log","mock.log"]:
                assert secret not in (logs/name).read_text()
        for line in metrics.splitlines():
            if line.startswith("duro_") and "{" in line:
                labels=line.split("{",1)[1].split("}",1)[0]
                assert "object_id=" not in labels and "request_id=" not in labels
        events=[json.loads(row) for i in [0,1] for row in (logs/f"router{i}.log").read_text().splitlines()]
        attempts=[e for e in events if e.get("request_id")=="postcommit"]
        assert {e["code"] for e in attempts}=={"UNAVAILABLE","OK"}
        for e in events:
            assert not set(e).intersection(("argument","input","kv","bundle","token"))
        (logs/"metrics.txt").write_text(metrics)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--policy",choices=["hrw","load"], default="hrw")
    parser.add_argument("--seed",type=int,default=421)
    parser.add_argument("--artifacts",type=Path,help="directory for process logs, timings and Prometheus snapshot")
    args=parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="duro-e2e-") as temp:
        tmp=Path(temp)
        logs=args.artifacts or Path(tempfile.mkdtemp(prefix="duro-results-"))
        logs.mkdir(parents=True,exist_ok=True)
        print(f"Artifacts: {logs}",flush=True)
        run(args,tmp,logs)
        print((logs/"scenarios.json").read_text())


if __name__=="__main__":
    main()
