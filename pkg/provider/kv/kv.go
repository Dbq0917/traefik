package kv

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/kvtools/valkeyrie"
	"github.com/kvtools/valkeyrie/store"
	"github.com/rs/zerolog/log"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/config/kv"
	"github.com/traefik/traefik/v3/pkg/job"
	"github.com/traefik/traefik/v3/pkg/observability/logs"
	"github.com/traefik/traefik/v3/pkg/safe"
)

// Provider holds configurations of the provider.
type Provider struct {
	RootKey string `description:"Root key used for KV store." json:"rootKey,omitempty" toml:"rootKey,omitempty" yaml:"rootKey,omitempty"`

	Endpoints []string `description:"KV store endpoints." json:"endpoints,omitempty" toml:"endpoints,omitempty" yaml:"endpoints,omitempty"`

	name           string
	kvClient       store.Store
	lastGoodMu     sync.RWMutex
	lastGoodConfig *dynamic.Configuration
}

// SetDefaults sets the default values.
func (p *Provider) SetDefaults() {
	p.RootKey = "traefik"
}

// Init the provider.
func (p *Provider) Init(storeType, name string, config valkeyrie.Config) error {
	ctx := log.With().Str(logs.ProviderName, name).Logger().WithContext(context.Background())

	p.name = name

	kvClient, err := p.createKVClient(ctx, storeType, config)
	if err != nil {
		return fmt.Errorf("failed to Connect to KV store: %w", err)
	}

	p.kvClient = kvClient

	return nil
}

// Provide allows the docker provider to provide configurations to traefik using the given configuration channel.
func (p *Provider) Provide(configurationChan chan<- dynamic.Message, pool *safe.Pool) error {
	logger := log.With().Str(logs.ProviderName, p.name).Logger()
	ctx := logger.WithContext(context.Background())

	operation := func() error {
		if _, err := p.kvClient.Exists(ctx, path.Join(p.RootKey, "qmslkjdfmqlskdjfmqlksjazcueznbvbwzlkajzebvkwjdcqmlsfj"), nil); err != nil {
			return fmt.Errorf("KV store connection error: %w", err)
		}
		return nil
	}

	notify := func(err error, time time.Duration) {
		logger.Error().
			Str("event", "kv_connection_unavailable").
			Dur("retryIn", time).
			Err(err).
			Msg("KV connection error, retrying")
	}

	err := backoff.RetryNotify(safe.OperationWithRecover(operation), backoff.WithContext(newRetryBackOff(), ctx), notify)
	if err != nil {
		return fmt.Errorf("cannot connect to KV server: %w", err)
	}

	configuration, err := p.buildConfiguration(ctx)
	if err != nil {
		logger.Error().
			Str("event", "kv_configuration_unavailable").
			Err(err).
			Msg("Cannot build the configuration")
	} else {
		configurationChan <- dynamic.Message{
			ProviderName:  p.name,
			Configuration: configuration,
		}
	}

	pool.GoCtx(func(ctxPool context.Context) {
		ctxLog := logger.With().Str(logs.ProviderName, p.name).Logger().WithContext(ctxPool)

		err := p.watchKv(ctxLog, configurationChan)
		if err != nil {
			logger.Error().Err(err).Msg("Cannot retrieve data")
		}
	})

	return nil
}

func (p *Provider) watchKv(ctx context.Context, configurationChan chan<- dynamic.Message) error {
	var (
		consecutiveFailures int
		fallbackPublished   bool
		degraded            bool
	)

	publish := func(configuration *dynamic.Configuration) {
		if configuration == nil {
			return
		}

		select {
		case configurationChan <- dynamic.Message{
			ProviderName:  p.name,
			Configuration: configuration,
		}:
		case <-ctx.Done():
			return
		}

		if degraded {
			log.Ctx(ctx).Info().
				Str("event", "kv_configuration_recovered").
				Int("consecutiveFailures", consecutiveFailures).
				Msg("KV configuration source recovered")
		}
		consecutiveFailures = 0
		fallbackPublished = false
		degraded = false
	}

	operation := func() error {
		events, err := p.kvClient.WatchTree(ctx, p.RootKey, nil)
		if err != nil {
			return fmt.Errorf("failed to watch KV: %w", err)
		}

		configuration, err := p.buildConfiguration(ctx)
		if err != nil {
			fallbackPublished = p.publishLastGoodConfiguration(ctx, configurationChan, fallbackPublished)
			return err
		}
		publish(configuration)

		for {
			select {
			case <-ctx.Done():
				return nil
			case _, ok := <-events:
				if !ok {
					return errors.New("the WatchTree channel is closed")
				}

				configuration, errC := p.buildConfiguration(ctx)
				if errC != nil {
					fallbackPublished = p.publishLastGoodConfiguration(ctx, configurationChan, fallbackPublished)
					return errC
				}

				publish(configuration)
			}
		}
	}

	notify := func(err error, time time.Duration) {
		consecutiveFailures++
		degraded = true

		if !fallbackPublished {
			fallbackPublished = p.publishLastGoodConfiguration(ctx, configurationChan, false)
		}

		log.Ctx(ctx).Error().
			Str("event", "kv_configuration_unavailable").
			Int("consecutiveFailures", consecutiveFailures).
			Dur("retryIn", time).
			Bool("usingLastKnownGood", p.hasLastKnownGood()).
			Bool("fallbackPublished", fallbackPublished).
			Err(err).
			Msg("KV configuration source unavailable, retrying")
	}

	return backoff.RetryNotify(safe.OperationWithRecover(operation),
		backoff.WithContext(newRetryBackOff(), ctx), notify)
}

func (p *Provider) buildConfiguration(ctx context.Context) (*dynamic.Configuration, error) {
	pairs, err := p.kvClient.List(ctx, p.RootKey, nil)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			// This empty configuration satisfies the pkg/server/configurationwatcher.go isEmptyConfiguration func constraints,
			// and will not be discarded by the configuration watcher.
			cfg := &dynamic.Configuration{
				HTTP: &dynamic.HTTPConfiguration{
					Routers: make(map[string]*dynamic.Router),
				},
			}
			p.setLastGoodConfiguration(cfg)

			return cfg, nil
		}

		return nil, err
	}

	cfg := &dynamic.Configuration{}
	err = kv.Decode(pairs, cfg, p.RootKey)
	if err != nil {
		return nil, err
	}

	p.setLastGoodConfiguration(cfg)

	return cfg, nil
}

func (p *Provider) setLastGoodConfiguration(cfg *dynamic.Configuration) {
	p.lastGoodMu.Lock()
	defer p.lastGoodMu.Unlock()

	p.lastGoodConfig = cfg.DeepCopy()
}

func (p *Provider) publishLastGoodConfiguration(ctx context.Context, configurationChan chan<- dynamic.Message, alreadyPublished bool) bool {
	if alreadyPublished || configurationChan == nil {
		return alreadyPublished
	}

	p.lastGoodMu.RLock()
	cfg := p.lastGoodConfig
	p.lastGoodMu.RUnlock()
	if cfg == nil {
		return false
	}

	select {
	case configurationChan <- dynamic.Message{
		ProviderName:  p.name,
		Configuration: cfg.DeepCopy(),
	}:
		return true
	case <-ctx.Done():
		return false
	default:
		return false
	}
}

func (p *Provider) hasLastKnownGood() bool {
	p.lastGoodMu.RLock()
	defer p.lastGoodMu.RUnlock()

	return p.lastGoodConfig != nil
}

func newRetryBackOff() *job.BackOff {
	exponential := backoff.NewExponentialBackOff()
	exponential.InitialInterval = 500 * time.Millisecond
	exponential.RandomizationFactor = 0.5
	exponential.Multiplier = 2
	// The 50% jitter keeps the effective retry interval below the 30s ceiling.
	exponential.MaxInterval = 20 * time.Second

	return job.NewBackOff(exponential)
}

func (p *Provider) createKVClient(ctx context.Context, storeType string, config valkeyrie.Config) (store.Store, error) {
	kvStore, err := valkeyrie.NewStore(ctx, storeType, p.Endpoints, config)
	if err != nil {
		return nil, err
	}

	return &storeWrapper{Store: kvStore}, nil
}
