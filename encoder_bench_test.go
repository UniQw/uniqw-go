package uniqw

import (
	"bytes"
	"encoding/json"
	"testing"
)

type benchSmall struct {
	A int    `json:"a"`
	B string `json:"b"`
	C bool   `json:"c"`
}

type benchMedium struct {
	I   int            `json:"i"`
	S   string         `json:"s"`
	B   bool           `json:"b"`
	F   float64        `json:"f"`
	Arr []int          `json:"arr"`
	M   map[string]int `json:"m"`
	N   struct {
		X string `json:"x"`
		Y int    `json:"y"`
	} `json:"n"`
}

type benchLarge struct {
	Name    string            `json:"name"`
	Tags    []string          `json:"tags"`
	Scores  []int             `json:"scores"`
	Payload []byte            `json:"payload"`
	Meta    map[string]string `json:"meta"`
}

func makeSmall() any {
	return benchSmall{A: 42, B: "hello", C: true}
}

func makeMedium() any {
	return benchMedium{
		I:   123,
		S:   "some-string-value",
		B:   true,
		F:   3.14159,
		Arr: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		M:   map[string]int{"a": 1, "b": 2, "c": 3},
		N: struct {
			X string `json:"x"`
			Y int    `json:"y"`
		}{X: "xval", Y: 9},
	}
}

func makeLarge() any {
	tags := make([]string, 50)
	for i := range tags {
		tags[i] = "tag-" + string(rune('a'+(i%26)))
	}
	scores := make([]int, 256)
	for i := range scores {
		scores[i] = i
	}
	payload := bytes.Repeat([]byte{0xAB}, 2048)
	meta := map[string]string{
		"k1": "v1", "k2": "v2", "k3": "v3", "k4": "v4", "k5": "v5",
	}
	return benchLarge{
		Name:    "large-object",
		Tags:    tags,
		Scores:  scores,
		Payload: payload,
		Meta:    meta,
	}
}

func BenchmarkJSONEncoder_Encode(b *testing.B) {
	cases := []struct {
		name string
		gen  func() any
	}{
		{"Small", makeSmall},
		{"Medium", makeMedium},
		{"Large", makeLarge},
	}

	enc := &JSONEncoder{}
	for _, cse := range cases {
		b.Run(cse.name, func(b *testing.B) {
			b.ReportAllocs()
			val := cse.gen()
			// warmup and estimate size
			warm, _ := enc.Encode(val)
			b.SetBytes(int64(len(warm)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := enc.Encode(val); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkJSONEncoder_Decode(b *testing.B) {
	cases := []struct {
		name   string
		gen    func() any
		newDst func() any
	}{
		{"Small", makeSmall, func() any { return new(benchSmall) }},
		{"Medium", makeMedium, func() any { return new(benchMedium) }},
		{"Large", makeLarge, func() any { return new(benchLarge) }},
	}
	enc := &JSONEncoder{}
	for _, cse := range cases {
		b.Run(cse.name, func(b *testing.B) {
			src := cse.gen()
			data, _ := enc.Encode(src)
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst := cse.newDst()
				if err := enc.Decode(data, dst); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Baseline using stdlib json directly (useful for relative comparisons)
func BenchmarkStdlibJSON_Encode_Small(b *testing.B) {
	v := makeSmall()
	// pre-size using Marshal once
	warm, _ := json.Marshal(v)
	b.ReportAllocs()
	b.SetBytes(int64(len(warm)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(v); err != nil {
			b.Fatal(err)
		}
	}
}
