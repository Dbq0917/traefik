package kv

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kvtools/valkeyrie/store"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
)

func TestBuildConfigurationRateLimitDoesNotDependOnPairOrder(t *testing.T) {
	pairs := rateLimitTestPairs()
	reversed := make([]*store.KVPair, len(pairs))
	for i, pair := range pairs {
		reversed[len(pairs)-1-i] = pair
	}

	firstProvider := newProviderMock(pairs)
	first, err := firstProvider.buildConfiguration(t.Context())
	require.NoError(t, err)

	secondProvider := newProviderMock(reversed)
	second, err := secondProvider.buildConfiguration(t.Context())
	require.NoError(t, err)

	assert.Equal(t, first, second)
	assert.Equal(t, int64(100), first.HTTP.Middlewares["rate-limit"].RateLimit.Average)
	assert.Equal(t, int64(200), first.HTTP.Middlewares["rate-limit"].RateLimit.Burst)
}

func TestWatchKvUsesLastKnownGoodConfigurationAndAlerts(t *testing.T) {
	logs := &synchronizedBuffer{}
	ctx, cancel := context.WithCancel(zerolog.New(zerolog.SyncWriter(logs)).WithContext(t.Context()))
	t.Cleanup(cancel)

	provider := newProviderMock(rateLimitTestPairs())
	provider.name = "redis"

	lastGood, err := provider.buildConfiguration(ctx)
	require.NoError(t, err)

	watchChannels := make(chan chan []*store.KVPair, 2)
	provider.kvClient.(*Mock).WatchTreeMethod = func() <-chan []*store.KVPair {
		channel := make(chan []*store.KVPair, 1)
		watchChannels <- channel
		return channel
	}
	provider.kvClient.(*Mock).SetListError(errors.New("store unavailable"))

	configChan := make(chan dynamic.Message, 1)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		_ = provider.watchKv(ctx, configChan)
	}()

	channel := receiveChannel(t, watchChannels)
	channel <- []*store.KVPair{}

	select {
	case msg := <-configChan:
		assert.Equal(t, "redis", msg.ProviderName)
		assert.Equal(t, lastGood, msg.Configuration)
	case <-time.After(time.Second):
		t.Fatal("last known good configuration was not published")
	}

	require.Eventually(t, func() bool {
		return strings.Contains(logs.String(), "kv_configuration_unavailable") &&
			strings.Contains(logs.String(), `"usingLastKnownGood":true`)
	}, time.Second, 10*time.Millisecond)

	cancel()
	select {
	case <-watchDone:
	case <-time.After(time.Second):
		t.Fatal("watchKv did not stop")
	}
}

func TestWatchKvWithoutLastKnownGoodDoesNotPublishConfiguration(t *testing.T) {
	logs := &synchronizedBuffer{}
	ctx, cancel := context.WithCancel(zerolog.New(zerolog.SyncWriter(logs)).WithContext(t.Context()))
	t.Cleanup(cancel)

	provider := newProviderMock(nil)
	provider.name = "redis"

	watchChannels := make(chan chan []*store.KVPair, 2)
	provider.kvClient.(*Mock).WatchTreeMethod = func() <-chan []*store.KVPair {
		channel := make(chan []*store.KVPair, 1)
		watchChannels <- channel
		return channel
	}
	provider.kvClient.(*Mock).SetListError(errors.New("store unavailable"))

	configChan := make(chan dynamic.Message, 1)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		_ = provider.watchKv(ctx, configChan)
	}()

	channel := receiveChannel(t, watchChannels)
	channel <- []*store.KVPair{}

	select {
	case <-configChan:
		t.Fatal("configuration must not be published without a valid rule")
	case <-time.After(100 * time.Millisecond):
	}

	require.Eventually(t, func() bool {
		return strings.Contains(logs.String(), "kv_configuration_unavailable") &&
			strings.Contains(logs.String(), `"usingLastKnownGood":false`)
	}, time.Second, 10*time.Millisecond)

	cancel()
	select {
	case <-watchDone:
	case <-time.After(time.Second):
		t.Fatal("watchKv did not stop")
	}
}

func TestWatchKvAppliesRateLimitChangeImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	provider := newProviderMock(nil)
	mock := provider.kvClient.(*Mock)

	initialPairs := rateLimitTestPairs()
	updatedPairs := rateLimitTestPairs()
	for _, pair := range updatedPairs {
		if strings.HasSuffix(pair.Key, "/average") {
			pair.Value = []byte("50")
		}
	}

	var updated atomic.Bool
	mock.SetListFunc(func(context.Context, string) ([]*store.KVPair, error) {
		if updated.Load() {
			return updatedPairs, nil
		}

		return initialPairs, nil
	})

	watchChannels := make(chan chan []*store.KVPair, 2)
	mock.WatchTreeMethod = func() <-chan []*store.KVPair {
		channel := make(chan []*store.KVPair, 1)
		watchChannels <- channel
		return channel
	}

	configChan := make(chan dynamic.Message, 1)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		_ = provider.watchKv(ctx, configChan)
	}()

	channel := receiveChannel(t, watchChannels)
	first := receiveConfiguration(t, configChan)
	require.Equal(t, int64(100), first.HTTP.Middlewares["rate-limit"].RateLimit.Average)

	updated.Store(true)
	channel <- []*store.KVPair{}

	second := receiveConfiguration(t, configChan)
	assert.Equal(t, int64(50), second.HTTP.Middlewares["rate-limit"].RateLimit.Average)

	cancel()
	select {
	case <-watchDone:
	case <-time.After(time.Second):
		t.Fatal("watchKv did not stop")
	}
}

func TestBuildConfigurationKeyNotFoundClearsLastKnownGood(t *testing.T) {
	provider := newProviderMock(rateLimitTestPairs())

	_, err := provider.buildConfiguration(t.Context())
	require.NoError(t, err)

	provider.kvClient.(*Mock).SetListError(store.ErrKeyNotFound)

	cfg, err := provider.buildConfiguration(t.Context())
	require.NoError(t, err)
	assert.Empty(t, cfg.HTTP.Middlewares)

	provider.lastGoodMu.RLock()
	defer provider.lastGoodMu.RUnlock()
	assert.Empty(t, provider.lastGoodConfig.HTTP.Middlewares)
}

func TestRetryBackOffIsBounded(t *testing.T) {
	retryBackOff := newRetryBackOff()
	assert.Equal(t, 20*time.Second, retryBackOff.MaxInterval)

	for range 20 {
		delay := retryBackOff.NextBackOff()
		require.Positive(t, delay)
		// The base 20s cap plus 50% jitter has a hard 30s ceiling.
		assert.LessOrEqual(t, delay, 30*time.Second)
	}
}

func rateLimitTestPairs() []*store.KVPair {
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

func receiveChannel(t *testing.T, channels <-chan chan []*store.KVPair) chan []*store.KVPair {
	t.Helper()

	select {
	case channel := <-channels:
		return channel
	case <-time.After(time.Second):
		t.Fatal("WatchTree was not called")
	}

	return nil
}

func receiveConfiguration(t *testing.T, configurations <-chan dynamic.Message) *dynamic.Configuration {
	t.Helper()

	select {
	case message := <-configurations:
		return message.Configuration
	case <-time.After(time.Second):
		t.Fatal("configuration was not published")
	}

	return nil
}

type synchronizedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}
