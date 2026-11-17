package kv

import (
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/kvtools/valkeyrie/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ptypes "github.com/traefik/paerser/types"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
)

func TestDecodeRateLimitDoesNotDependOnPairOrder(t *testing.T) {
	pairs := rateLimitPairs()
	reversed := slices.Clone(pairs)
	slices.Reverse(reversed)

	first, err := decodeRateLimit(pairs)
	require.NoError(t, err)

	second, err := decodeRateLimit(reversed)
	require.NoError(t, err)

	assert.Equal(t, first, second)
}

func TestDecodeRateLimitPreservesNumericSliceOrder(t *testing.T) {
	rule, err := decodeRateLimit(rateLimitPairs())
	require.NoError(t, err)

	var expected []string
	for i := range 12 {
		expected = append(expected, "10.0.0."+strconv.Itoa(i))
	}

	require.NotNil(t, rule.SourceCriterion)
	require.NotNil(t, rule.SourceCriterion.IPStrategy)
	assert.Equal(t, expected, rule.SourceCriterion.IPStrategy.ExcludedIPs)
}

func TestDecodeRateLimitDoesNotMutateInput(t *testing.T) {
	pairs := rateLimitPairs()
	before := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		before = append(before, pair.Key)
	}

	_, err := decodeRateLimit(pairs)
	require.NoError(t, err)

	after := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		after = append(after, pair.Key)
	}

	assert.Equal(t, before, after)
}

func decodeRateLimit(pairs []*store.KVPair) (*dynamic.RateLimit, error) {
	var cfg dynamic.Configuration
	if err := Decode(pairs, &cfg, "traefik"); err != nil {
		return nil, err
	}

	return cfg.HTTP.Middlewares["rate-limit"].RateLimit, nil
}

func TestDecodeRateLimitDefaults(t *testing.T) {
	rule, err := decodeRateLimit(rateLimitPairs())
	require.NoError(t, err)

	assert.Equal(t, int64(100), rule.Average)
	assert.Equal(t, int64(200), rule.Burst)
	assert.Equal(t, ptypes.Duration(time.Second), rule.Period)
}
