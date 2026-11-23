package jsonvalue

import (
	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"testing"
)

func TestOperationParameters(t *testing.T) {
	value := func(s string) *pb.JsonValue {
		if s == "" {
			return nil
		}
		return &pb.JsonValue{Utf8: []byte(s)}
	}
	for _, tc := range []struct {
		left, right string
		equal       bool
	}{
		{"", "", true}, {"", "null", false}, {"null", "null", true},
		{`{"x":0.1,"y":[1,2]}`, `{"y":[1e0,2.0],"x":1E-1}`, true},
		{"9007199254740992", "9007199254740993", false}, {"-0", "0.0e+9999999999", true},
		{"1e999999999999999999999", "10e999999999999999999998", true},
		{"[1,2]", "[2,1]", false}, {"1", `"1"`, false},
		{`{"x":1,"x":2}`, `{"x":2}`, false},
	} {
		if got := Equal(value(tc.left), value(tc.right)); got != tc.equal {
			t.Errorf("Equal(%q,%q) = %t", tc.left, tc.right, got)
		}
	}
	for _, invalid := range []string{`{"a":[{"b":1,"\u0062":2}]}`, "null null", string([]byte{'"', 255, '"'})} {
		if Valid(value(invalid)) {
			t.Errorf("accepted invalid JSON %q", invalid)
		}
	}
}
