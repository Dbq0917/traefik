package kv

import (
	"strconv"
	"testing"

	"github.com/kvtools/valkeyrie/store"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
)

func BenchmarkDecodeRateLimit(b *testing.B) {
	pairs := rateLimitPairs()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		input := make([]*store.KVPair, len(pairs))
		for i, pair := range pairs {
			input[i] = &store.KVPair{Key: pair.Key, Value: pair.Value}
		}

		var cfg dynamic.Configuration
		if err := Decode(input, &cfg, "traefik"); err != nil {
			b.Fatal(err)
		}
	}
}

func rateLimitPairs() []*store.KVPair {
	pairs := []*store.KVPair{
		{Key: "traefik/http/middlewares/rate-limit/rateLimit/average", Value: []byte("100")},
		{Key: "traefik/http/middlewares/rate-limit/rateLimit/period", Value: []byte("1s")},
		{Key: "traefik/http/middlewares/rate-limit/rateLimit/burst", Value: []byte("200")},
		{Key: "traefik/http/middlewares/rate-limit/rateLimit/sourceCriterion/requestHeaderName", Value: []byte("X-API-Key")},
	}

	for i := range 12 {
		pairs = append(pairs, &store.KVPair{
			Key:   "traefik/http/middlewares/rate-limit/rateLimit/sourceCriterion/ipStrategy/excludedIPs/" + strconv.Itoa(i),
			Value: []byte("10.0.0." + strconv.Itoa(i)),
		})
	}

	return pairs
}
