// Package nsq provides an NSQ broker
package nsq

import (
	"context"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/broker/codec/json"
	"github.com/micro/go-micro/cmd"

	"github.com/pborman/uuid"
	nsq "github.com/youzan/go-nsq"
)

type nsqBroker struct {
	lookupdAddrs []string
	addrs        []string
	opts         broker.Options
	config       *nsq.Config

	sync.Mutex
	running bool
	p       []*nsq.Producer
	c       []*subscriber

	mgr *nsq.TopicProducerMgr
}

type publication struct {
	topic string
	m     *broker.Message
	nm    *nsq.Message
	opts  broker.PublishOptions
}

type subscriber struct {
	topic string
	opts  broker.SubscribeOptions

	c *nsq.Consumer

	// handler so we can resubcribe
	h nsq.HandlerFunc
	// concurrency
	n int
}

var (
	DefaultConcurrentHandlers = 1
	DefaultQueueName          = ""
)

func init() {
	rand.Seed(time.Now().UnixNano())
	cmd.DefaultBrokers["youzan"] = NewBroker
}

func (n *nsqBroker) Init(opts ...broker.Option) error {
	for _, o := range opts {
		o(&n.opts)
	}

	n.initByContext(n.opts.Context)

	return nil
}

func (n *nsqBroker) initByContext(ctx context.Context) {
	if v, ok := ctx.Value(lookupdAddrsKey{}).([]string); ok {
		n.lookupdAddrs = v
	}

	if v, ok := ctx.Value(consumerOptsKey{}).([]string); ok {
		cfgFlag := &nsq.ConfigFlag{Config: n.config}
		for _, opt := range v {
			cfgFlag.Set(opt)
		}
	}
}

func (n *nsqBroker) Options() broker.Options {
	return n.opts
}

func (n *nsqBroker) Address() string {
	return n.addrs[rand.Intn(len(n.addrs))]
}

func (n *nsqBroker) Connect() error {
	n.Lock()
	defer n.Unlock()

	if n.running {
		return nil
	}

	n.config = nsq.NewConfig()

	pubMgr, err := nsq.NewTopicProducerMgr([]string{}, n.config)
	if err != nil {
		log.Printf("nsqBroker NewTopicProducerMgr error : %v", err)
		return err
	}
	pubMgr.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)

	pubMgr.AddLookupdNodes(n.addrs)

	n.mgr = pubMgr

	n.running = true
	return nil
}

func (n *nsqBroker) Disconnect() error {
	n.Lock()
	defer n.Unlock()

	if !n.running {
		return nil
	}
	n.mgr.Stop()
	n.running = false
	return nil
}

func (n *nsqBroker) Publish(topic string, message *broker.Message, opts ...broker.PublishOption) error {

	options := broker.PublishOptions{}
	for _, o := range opts {
		o(&options)
	}

	var (
		doneChan chan *nsq.ProducerTransaction
		delay    time.Duration
	)
	if options.Context != nil {
		if v, ok := options.Context.Value(asyncPublishKey{}).(chan *nsq.ProducerTransaction); ok {
			doneChan = v
		}
		if v, ok := options.Context.Value(deferredPublishKey{}).(time.Duration); ok {
			delay = v
		}
	}

	if delay > 0 {
		message.Header["_delay"] = time.Now().Add(delay).Format(time.RFC3339)
	}

	b, err := n.opts.Codec.Marshal(message)
	if err != nil {
		return err
	}

	if doneChan != nil {
		return n.mgr.PublishAsync(topic, b, doneChan)
	} else {
		return n.mgr.Publish(topic, b)
	}
}

func (n *nsqBroker) Subscribe(topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	options := broker.SubscribeOptions{
		AutoAck: true,
		Queue:   DefaultQueueName,
	}

	for _, o := range opts {
		o(&options)
	}

	concurrency, maxInFlight := DefaultConcurrentHandlers, DefaultConcurrentHandlers
	if options.Context != nil {
		if v, ok := options.Context.Value(concurrentHandlerKey{}).(int); ok {
			maxInFlight, concurrency = v, v
		}
		if v, ok := options.Context.Value(maxInFlightKey{}).(int); ok {
			maxInFlight = v
		}
	}
	channel := options.Queue
	if len(channel) == 0 {
		channel = uuid.NewUUID().String() + "#ephemeral"
	}
	config := *n.config
	config.MaxInFlight = maxInFlight

	c, err := nsq.NewConsumer(topic, channel, &config)
	if err != nil {
		return nil, err
	}

	h := nsq.HandlerFunc(func(nm *nsq.Message) error {
		var isDisable bool
		if !options.AutoAck {
			isDisable = true
			nm.DisableAutoResponse()
		}

		var m broker.Message

		if err := n.opts.Codec.Unmarshal(nm.Body, &m); err != nil {
			return err
		}

		//  处理延时消息
		if m.Header != nil {
			if delay, has := m.Header["_delay"]; has {
				if !isDisable {
					nm.DisableAutoResponse()
				}
				delayTs, err := time.Parse(time.RFC3339, delay)
				if err != nil {
					return err
				}
				now := time.Now()
				if delayTs.After(now) {
					nm.RequeueWithoutBackoff(delayTs.Sub(now))
				}

			}
		}

		return handler(&publication{
			topic: topic,
			m:     &m,
			nm:    nm,
		})
	})

	c.AddConcurrentHandlers(h, concurrency)

	err = c.ConnectToNSQLookupds(n.addrs)
	if err != nil {
		return nil, err
	}

	sub := &subscriber{
		c:     c,
		opts:  options,
		topic: topic,
		h:     h,
		n:     concurrency,
	}

	n.c = append(n.c, sub)

	return sub, nil
}

func (n *nsqBroker) String() string {
	return "youzan"
}

func (p *publication) Topic() string {
	return p.topic
}

func (p *publication) Message() *broker.Message {
	return p.m
}

func (p *publication) Ack() error {
	p.nm.Finish()
	return nil
}

func (s *subscriber) Options() broker.SubscribeOptions {
	return s.opts
}

func (s *subscriber) Topic() string {
	return s.topic
}

func (s *subscriber) Unsubscribe() error {
	s.c.Stop()
	return nil
}

func NewBroker(opts ...broker.Option) broker.Broker {
	options := broker.Options{
		// Default codec
		Codec: json.NewCodec(),
		// Default context
		Context: context.Background(),
	}

	for _, o := range opts {
		o(&options)
	}

	var addrs []string

	for _, addr := range options.Addrs {
		if len(addr) > 0 {
			addrs = append(addrs, addr)
		}
	}

	if len(addrs) == 0 {
		addrs = []string{"127.0.0.1:4150"}
	}

	return &nsqBroker{
		addrs:  addrs,
		opts:   options,
		config: nsq.NewConfig(),
	}
}
