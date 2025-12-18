package keys

import "testing"

func BenchmarkFor(b *testing.B) {
	b.ReportAllocs()
	var sink Queue
	for i := 0; i < b.N; i++ {
		sink = For("email")
	}
	_ = sink
}

func BenchmarkBuilders(b *testing.B) {
	cases := []struct {
		name string
		fn   func(string) string
	}{
		{"Pending", Pending},
		{"Active", Active},
		{"Delayed", Delayed},
		{"Dead", Dead},
		{"Succeeded", Succeeded},
		{"Unique", Unique},
		{"Expiry", Expiry},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			var s string
			for i := 0; i < b.N; i++ {
				s = c.fn("video-jobs")
			}
			_ = s
		})
	}
}
