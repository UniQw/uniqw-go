package uniqw

import (
	"bytes"
	"testing"
)

func makeBenchTask(payloadSize int) Task {
	return Task{
		ID:         "id-123",
		Type:       "email.send",
		Queue:      "emails",
		Payload:    bytes.Repeat([]byte("x"), payloadSize),
		Retry:      1,
		MaxRetry:   5,
		Retention:  60,
		CreatedAt:  1730000000000,
		DeadlineMs: 1730000005000,
		StartedAt:  0,
	}
}

func BenchmarkTask_JSON_Encode(b *testing.B) {
	sizes := []int{64, 512, 2048}
	enc := &JSONEncoder{}
	for _, sz := range sizes {
		b.Run(byteSizeName(sz), func(b *testing.B) {
			b.ReportAllocs()
			t := makeBenchTask(sz)
			warm, _ := enc.Encode(t)
			b.SetBytes(int64(len(warm)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := enc.Encode(t); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTask_JSON_Decode(b *testing.B) {
	sizes := []int{64, 512, 2048}
	enc := &JSONEncoder{}
	for _, sz := range sizes {
		b.Run(byteSizeName(sz), func(b *testing.B) {
			src := makeBenchTask(sz)
			data, _ := enc.Encode(src)
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var dst Task
				if err := enc.Decode(data, &dst); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func byteSizeName(n int) string {
	switch {
	case n < 1024:
		return "" + itoa(n) + "B"
	case n < 1024*1024:
		return itoa(n/1024) + "KB"
	default:
		return itoa(n/(1024*1024)) + "MB"
	}
}

// lightweight int->string without fmt to reduce noise in bench labels
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + (n % 10))
		n /= 10
	}
	return string(buf[i:])
}
