package kv

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/kvtools/valkeyrie/store"
)

func newProviderMock(kvPairs []*store.KVPair) *Provider {
	return &Provider{
		RootKey:  "traefik",
		kvClient: newKvClientMock(kvPairs, nil),
	}
}

// Override Get/List to return a error.
type KvError struct {
	Get  error
	List error
}

// Extremely limited mock store so we can test initialization.
type Mock struct {
	Error           KvError
	KVPairs         []*store.KVPair
	WatchTreeMethod func() <-chan []*store.KVPair
	ListFunc        func(context.Context, string) ([]*store.KVPair, error)
	ListCalls       atomic.Int64

	mu sync.RWMutex
}

func newKvClientMock(kvPairs []*store.KVPair, err error) *Mock {
	mock := &Mock{
		KVPairs: kvPairs,
	}

	if err != nil {
		mock.Error = KvError{
			Get:  err,
			List: err,
		}
	}
	return mock
}

func (s *Mock) Put(ctx context.Context, key string, value []byte, opts *store.WriteOptions) error {
	return errors.New("method Put not supported")
}

func (s *Mock) Get(ctx context.Context, key string, options *store.ReadOptions) (*store.KVPair, error) {
	if err := s.Error.Get; err != nil {
		return nil, err
	}
	for _, kvPair := range s.KVPairs {
		if kvPair.Key == key {
			return kvPair, nil
		}
	}
	return nil, store.ErrKeyNotFound
}

func (s *Mock) Delete(ctx context.Context, key string) error {
	return errors.New("method Delete not supported")
}

// Exists mock.
func (s *Mock) Exists(ctx context.Context, key string, options *store.ReadOptions) (bool, error) {
	if err := s.Error.Get; err != nil {
		return false, err
	}
	for _, kvPair := range s.KVPairs {
		if strings.HasPrefix(kvPair.Key, key) {
			return true, nil
		}
	}
	return false, store.ErrKeyNotFound
}

// Watch mock.
func (s *Mock) Watch(ctx context.Context, key string, options *store.ReadOptions) (<-chan *store.KVPair, error) {
	return nil, errors.New("method Watch not supported")
}

// WatchTree mock.
func (s *Mock) WatchTree(ctx context.Context, prefix string, options *store.ReadOptions) (<-chan []*store.KVPair, error) {
	return s.WatchTreeMethod(), nil
}

// NewLock mock.
func (s *Mock) NewLock(ctx context.Context, key string, options *store.LockOptions) (store.Locker, error) {
	return nil, errors.New("method NewLock not supported")
}

// List mock.
func (s *Mock) List(ctx context.Context, prefix string, options *store.ReadOptions) ([]*store.KVPair, error) {
	s.ListCalls.Add(1)

	s.mu.RLock()
	listFunc := s.ListFunc
	listErr := s.Error.List
	kvPairs := s.KVPairs
	s.mu.RUnlock()

	if listFunc != nil {
		return listFunc(ctx, prefix)
	}

	if listErr != nil {
		return nil, listErr
	}

	var kv []*store.KVPair
	for _, kvPair := range kvPairs {
		if strings.HasPrefix(kvPair.Key, prefix) {
			kv = append(kv, kvPair)
		}
	}
	return kv, nil
}

func (s *Mock) SetListError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Error.List = err
}

func (s *Mock) SetListFunc(listFunc func(context.Context, string) ([]*store.KVPair, error)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ListFunc = listFunc
}

// DeleteTree mock.
func (s *Mock) DeleteTree(ctx context.Context, prefix string) error {
	return errors.New("method DeleteTree not supported")
}

// AtomicPut mock.
func (s *Mock) AtomicPut(ctx context.Context, key string, value []byte, previous *store.KVPair, opts *store.WriteOptions) (bool, *store.KVPair, error) {
	return false, nil, errors.New("method AtomicPut not supported")
}

// AtomicDelete mock.
func (s *Mock) AtomicDelete(ctx context.Context, key string, previous *store.KVPair) (bool, error) {
	return false, errors.New("method AtomicDelete not supported")
}

// Close mock.
func (s *Mock) Close() error {
	return nil
}
