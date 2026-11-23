package teststand

import (
	"encoding/json"
	"math"
	"slices"
	"strconv"
	"strings"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"github.com/duro-d/duro/routing/internal/jsonvalue"
)

type auction struct {
	Price  int64   `json:"price"`
	Bidder *string `json:"bidder"`
	Closed bool    `json:"closed"`
}

func encode(v any) *pb.JsonValue {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return &pb.JsonValue{Utf8: raw}
}
func ok(v *pb.JsonValue) *pb.OperationResult {
	return &pb.OperationResult{Outcome: &pb.OperationResult_Result{Result: v}}
}
func reject(reason string) *pb.OperationResult {
	return &pb.OperationResult{Outcome: &pb.OperationResult_DomainRejection{DomainRejection: reason}}
}
func fields(arg *pb.JsonValue) map[string]json.RawMessage {
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(arg.GetUtf8(), &fields)
	return fields
}
func integer(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	v, err := strconv.ParseFloat(string(raw), 64)
	if err != nil || math.Abs(v) > 9007199254740991 || math.Trunc(v) != v {
		return 0, false
	}
	i := int64(v)
	return i, jsonvalue.Equal(&pb.JsonValue{Utf8: raw}, encode(i))
}
func fixtureMethods(typ string) []string {
	switch strings.ToLower(typ) {
	case "auction":
		return []string{"bid", "close", "getSummary"}
	case "quota":
		return []string{"spend", "getRemaining"}
	}
	return nil
}
func checkFixture(p *pb.CodePackage) error {
	if err := validate(p); err != nil {
		return err
	}
	allowed := fixtureMethods(p.Manifest.Code.Type)
	if allowed == nil {
		return invalid("mock supports Auction and Quota")
	}
	for _, method := range p.Manifest.Methods {
		if !slices.Contains(allowed, method) {
			return invalid("method is outside the mock fixture")
		}
	}
	return nil
}
func initFixture(op *pb.CreateOperation) (map[string]*pb.JsonValue, *pb.OperationResult) {
	f := fields(op.Input)
	if strings.EqualFold(op.Type, "Auction") {
		price, valid := integer(f["startPrice"])
		if !valid || price < 0 {
			return nil, reject("Некорректная начальная цена")
		}
		return map[string]*pb.JsonValue{"auction": encode(auction{Price: price})}, ok(encode(nil))
	}
	limit, valid := integer(f["limit"])
	if !valid || limit < 0 {
		return nil, reject("Invalid quota")
	}
	return map[string]*pb.JsonValue{"remaining": encode(limit)}, ok(encode(nil))
}
func runFixture(o *object, op *pb.InvokeOperation) (map[string]*pb.JsonValue, *pb.OperationResult, error) {
	if !slices.Contains(o.code.Manifest.Methods, op.Method) {
		return nil, nil, missing("unknown method")
	}
	f := fields(op.Argument)
	switch op.Method {
	case "getSummary":
		return o.kv, ok(o.kv["auction"]), nil
	case "close", "bid":
		var a auction
		if err := json.Unmarshal(o.kv["auction"].Utf8, &a); err != nil {
			panic(err)
		}
		if op.Method == "close" {
			a.Closed = true
		} else {
			if a.Closed {
				return nil, reject("Аукцион закрыт"), nil
			}
			var bidder string
			if json.Unmarshal(f["bidder"], &bidder) != nil || bidder == "" {
				return nil, reject("Не указан участник"), nil
			}
			amount, valid := integer(f["amount"])
			if !valid || amount <= a.Price {
				return nil, reject("Ставка должна быть целым числом выше текущей цены"), nil
			}
			a.Price = amount
			a.Bidder = &bidder
		}
		result := encode(a)
		return map[string]*pb.JsonValue{"auction": result}, ok(result), nil
	case "getRemaining", "spend":
		remaining, valid := integer(o.kv["remaining"].Utf8)
		if !valid {
			panic("invalid fixture balance")
		}
		if op.Method == "spend" {
			amount, valid := integer(f["amount"])
			if !valid || amount <= 0 || amount > remaining {
				return nil, reject("Insufficient quota"), nil
			}
			remaining -= amount
		}
		return map[string]*pb.JsonValue{"remaining": encode(remaining)}, ok(encode(map[string]int64{"remaining": remaining})), nil
	default:
		return nil, nil, missing("unknown fixture method")
	}
}
