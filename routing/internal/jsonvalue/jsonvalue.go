// Package jsonvalue compares JSON arguments carried by the routing protocol.
package jsonvalue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"unicode/utf8"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
)

// Valid accepts an omitted argument. Callers that require presence check it separately.
func Valid(v *pb.JsonValue) bool {
	if v == nil {
		return true
	}
	_, err := parse(v.Utf8)
	return err == nil
}

func Equal(a, b *pb.JsonValue) bool {
	if a == nil || b == nil {
		return a == b
	}
	left, err := parse(a.Utf8)
	if err != nil {
		return false
	}
	right, err := parse(b.Utf8)
	return err == nil && reflect.DeepEqual(left, right)
}

func parse(raw []byte) (any, error) {
	if !utf8.Valid(raw) || !json.Valid(raw) {
		return nil, fmt.Errorf("invalid UTF-8 JSON")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	return value(d)
}

func value(d *json.Decoder) (any, error) {
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch t := t.(type) {
	case json.Number:
		return normalize(string(t)), nil
	case json.Delim:
		if t == '{' {
			m := make(map[string]any)
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return nil, err
				}
				s := key.(string)
				if _, ok := m[s]; ok {
					return nil, fmt.Errorf("duplicate JSON key")
				}
				m[s], err = value(d)
				if err != nil {
					return nil, err
				}
			}
			_, err = d.Token()
			return m, err
		}
		a := make([]any, 0)
		for d.More() {
			child, err := value(d)
			if err != nil {
				return nil, err
			}
			a = append(a, child)
		}
		_, err = d.Token()
		return a, err
	default:
		return t, nil
	}
}

// Keep a decimal exponent separate so even 1e999999999 needs bounded memory.
func normalize(s string) json.Number {
	mantissa, exponent, hasExp := strings.Cut(strings.ToLower(s), "e")
	negative := strings.HasPrefix(mantissa, "-")
	mantissa = strings.TrimPrefix(mantissa, "-")
	integer, fraction, _ := strings.Cut(mantissa, ".")
	digits := strings.TrimLeft(integer+fraction, "0")
	if digits == "" {
		return "0"
	}
	significant := strings.TrimRight(digits, "0")
	var power big.Int
	if hasExp {
		power.SetString(exponent, 10)
	}
	power.Add(&power, big.NewInt(int64(len(digits)-len(significant)-len(fraction))))
	if negative {
		significant = "-" + significant
	}
	return json.Number(significant + "e" + power.String())
}
