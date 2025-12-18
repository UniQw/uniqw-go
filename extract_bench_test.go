package uniqw

import "testing"

func BenchmarkExtractQueueName(b *testing.B) {
	cases := []struct {
		name string
		key  string
	}{
		{"Pending", "uniqw:{email}:pending"},
		{"Active", "uniqw:{video}:active"},
		{"NoBraces", "uniqw:email:pending"},
		{"MissingRight", "uniqw:{email:pending"},
		{"MissingLeft", "uniqw:email}:pending"},
		{"Long", "uniqw:{a-very-long-queue-name-with-many-segments}:succeeded"},
		{"EmptyBraces", "uniqw:{}:pending"},
		{"JustBraces", "{}"},
		{"Empty", ""},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			var sink string
			for i := 0; i < b.N; i++ {
				sink = ExtractQueueName(c.key)
			}
			_ = sink
		})
	}
}
