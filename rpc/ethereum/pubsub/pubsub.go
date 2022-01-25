package pubsub

import (
	"github.com/ethereum/go-ethereum/log"
	"sync"
	"time"

	"github.com/pkg/errors"

	coretypes "github.com/tendermint/tendermint/rpc/core/types"
)

type EventBus interface {
	AddTopic(name string, src <-chan coretypes.ResultEvent) error
	RemoveTopic(name string)
	Subscribe(name string) (chan coretypes.ResultEvent, error)
	Topics() []string
	UnSubscribe(name string, ch chan coretypes.ResultEvent) (bool, error)
}

type memEventBus struct {
	topics         map[string]<-chan coretypes.ResultEvent
	topicsMux      *sync.RWMutex
	subscribers    map[string][]chan coretypes.ResultEvent
	subscribersMux *sync.RWMutex
}

func NewEventBus() EventBus {
	return &memEventBus{
		topics:         make(map[string]<-chan coretypes.ResultEvent),
		topicsMux:      new(sync.RWMutex),
		subscribers:    make(map[string][]chan coretypes.ResultEvent),
		subscribersMux: new(sync.RWMutex),
	}
}

func (m *memEventBus) Topics() (topics []string) {
	m.topicsMux.RLock()
	defer m.topicsMux.RUnlock()

	topics = make([]string, 0, len(m.topics))
	for topicName := range m.topics {
		topics = append(topics, topicName)
	}

	return topics
}

func (m *memEventBus) AddTopic(name string, src <-chan coretypes.ResultEvent) error {
	m.topicsMux.RLock()
	_, ok := m.topics[name]
	m.topicsMux.RUnlock()

	if ok {
		return errors.New("topic already registered")
	}

	m.topicsMux.Lock()
	m.topics[name] = src
	m.topicsMux.Unlock()

	go m.publishTopic(name, src)

	return nil
}

func (m *memEventBus) RemoveTopic(name string) {
	m.topicsMux.Lock()
	delete(m.topics, name)
	m.topicsMux.Unlock()
}

func (m *memEventBus) Subscribe(name string) (chan coretypes.ResultEvent, error) {
	m.topicsMux.RLock()
	_, ok := m.topics[name]
	m.topicsMux.RUnlock()

	if !ok {
		return nil, errors.Errorf("topic not found: %s", name)
	}

	ch := make(chan coretypes.ResultEvent)
	m.subscribersMux.Lock()
	defer m.subscribersMux.Unlock()
	m.subscribers[name] = append(m.subscribers[name], ch)

	return ch, nil
}

func (m *memEventBus) UnSubscribe(name string, ch chan coretypes.ResultEvent) (bool, error) {
	m.topicsMux.RLock()
	_, ok := m.topics[name]
	m.topicsMux.RUnlock()

	if !ok {
		return false, errors.Errorf("topic not found: %s", name)
	}
	m.subscribersMux.Lock()
	defer m.subscribersMux.Unlock()
	for i, v := range m.subscribers[name] {
		if v == ch {
			m.subscribers[name] = append(m.subscribers[name][:i], m.subscribers[name][i+1:]...)
			close(ch)
			return len(m.subscribers[name]) == 0 ,nil
		}
	}
	return false, errors.New("channel not exist")
}

func (m *memEventBus) publishTopic(name string, src <-chan coretypes.ResultEvent) {
	for {
		msg, ok := <-src
		if !ok {
			m.closeAllSubscribers(name)
			m.topicsMux.Lock()
			delete(m.topics, name)
			m.topicsMux.Unlock()
			return
		}
		m.publishAllSubscribers(name, msg)
	}
}

func (m *memEventBus) closeAllSubscribers(name string) {
	m.subscribersMux.Lock()
	defer m.subscribersMux.Unlock()

	subsribers := m.subscribers[name]
	delete(m.subscribers, name)

	for _, sub := range subsribers {
		close(sub)
	}
}

func (m *memEventBus) publishAllSubscribers(name string, msg coretypes.ResultEvent) {
	defer func() {
		if err := recover(); err != nil {
			log.Error("publishAllSubscribers err", err)
		}
	}()
	m.subscribersMux.RLock()
	subsribers := m.subscribers[name]
	m.subscribersMux.RUnlock()

	for _, sub := range subsribers {
		select {
		case sub <- msg:
		case <- time.After(500*time.Millisecond):
			log.Error("publish sub err ,time out")
		default:
		}
	}
}
