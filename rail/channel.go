package rail

import (
	"bytes"
	"container/heap"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/log"
	"github.com/tenfer/gorail/internal/pqueue"
)

const (
	RetryStategyEqual = iota
	RetryStategyGrow
)

type Handler interface {
	Handle(message *Message) error
	Close() error
}

type Channel struct {
	requeueCount uint64
	messageCount uint64
	timeoutCount uint64

	sync.RWMutex

	topicName      string
	name           string
	memoryMsgChan  chan *Message
	backend        BackendQueue
	ctx            *context
	concurrentNum  int //处理数据并发数
	deleteCallback func(*Channel)
	exitMutex      sync.RWMutex

	deferredMessages map[MessageID]*pqueue.Item
	deferredPQ       pqueue.PriorityQueue
	deferredMutex    sync.Mutex
	inFlightMessages map[MessageID]*Message
	inFlightPQ       inFlightPqueue
	inFlightMutex    sync.Mutex

	h  Handler //每个channel只能注册单个handler
	wg sync.WaitGroup

	exitChan  chan struct{}
	paused    int32
	pauseChan chan bool
	closing   int32

	retryStrategy    int //retry strategy
	retryIntervalSec int64
	retryMaxTimes    int
}

func NewChannel(topicName, channelName string, ctx *context, deleteCallback func(*Channel)) *Channel {
	c := &Channel{
		topicName: topicName,
		name:      channelName,
		//memoryMsgChan: make(chan *Message, ctx.rail.c.TopicConfig.MemBuffSize),
		memoryMsgChan:    nil,
		deleteCallback:   deleteCallback,
		ctx:              ctx,
		concurrentNum:    1,
		retryStrategy:    RetryStategyGrow,
		retryIntervalSec: 60, //1 min
		retryMaxTimes:    100,
	}

	c.initPQ()

	// backend names, for uniqueness, automatically include the topic...
	backendName := getBackendName(topicName, channelName)
	c.backend = newDiskQueue(backendName,
		ctx.rail.c.BackendConfig.DataPath,
		ctx.rail.c.BackendConfig.MaxBytesPerFile,
		1,
		ctx.rail.c.BackendConfig.MaxMsgSize,
		ctx.rail.c.BackendConfig.SyncEvery,
		ctx.rail.c.BackendConfig.SyncTimeout,
	)

	c.exitChan = make(chan struct{})

	return c
}

func (c *Channel) AddHandler(handler Handler) {
	c.Lock()
	defer c.Unlock()
	c.h = handler
}
func (c *Channel) GetHandler() Handler {
	c.RLock()
	defer c.RUnlock()
	return c.h
}

func (c *Channel) initPQ() {
	pqSize := int(math.Max(1, float64(c.ctx.rail.c.TopicConfig.MemBuffSize)/10))

	c.inFlightMessages = make(map[MessageID]*Message)
	c.deferredMessages = make(map[MessageID]*pqueue.Item)

	c.inFlightMutex.Lock()
	c.inFlightPQ = newInFlightPqueue(pqSize)
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	c.deferredPQ = pqueue.New(pqSize)
	c.deferredMutex.Unlock()
}

func (c *Channel) Push(m *Message) error {
	c.RLock()
	defer c.RUnlock()

	if c.Exiting() {
		return errors.New("exiting")
	}

	c.put(m)
	atomic.AddUint64(&c.messageCount, 1)
	return nil
}

func (c *Channel) put(m *Message) error {
	select {
	case c.memoryMsgChan <- m:
	default:
		buf := bufferPoolGet()
		err := writeMessageToBackend(buf, m, c.backend)
		bufferPoolPut(buf)

		if err != nil {
			b, _ := m.Encode2Json()
			log.Errorf("CHANNEL(%s): Message(%s) send to Backend Queue error.", c.name, string(b))
			return err
		}
	}
	return nil
}

func (c *Channel) Run() {
	log.Infof("CHANNEL(%s): start (%d) go-routines", c.name, c.concurrentNum)
	for i := 0; i < c.concurrentNum; i++ {
		c.wg.Add(1)
		go c.process()
	}
}

// Exiting returns a boolean indicating if this channel is closed/exiting
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.closing) == 1
}

// Delete empties the channel and closes
func (c *Channel) Delete() error {
	return c.exit(true)
}

// Close cleanly closes the Channel
func (c *Channel) Close() error {
	return c.exit(false)
}

func (c *Channel) exit(deleted bool) error {
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()

	if !atomic.CompareAndSwapInt32(&c.closing, 0, 1) {
		return errors.New("exiting")
	}

	close(c.exitChan)
	c.wg.Wait()

	//关闭handler
	c.h.Close()

	if deleted {
		log.Infof("CHANNEL(%s): deleting", c.name)
	} else {
		log.Infof("CHANNEL(%s): closing", c.name)
	}

	if deleted {
		// empty the queue (deletes the backend files, too)
		c.Empty()
		return c.backend.Delete()
	}

	// write anything leftover to disk
	c.flush()
	return c.backend.Close()
}

func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()

	c.initPQ()

	for {
		select {
		case <-c.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return c.backend.Empty()
}

func (c *Channel) flush() error {
	var msgBuf bytes.Buffer

	if len(c.memoryMsgChan) > 0 {
		log.Infof("CHANNEL(%s): flushing %d memory messages to backend",
			c.name, len(c.memoryMsgChan))
	}
	for {
		select {
		case msg := <-c.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, c.backend)
			if err != nil {
				bmsg, _ := msg.Encode2Json()
				log.Errorf("CHANNEL(%s): flush msg (%s) error.", c.name, string(bmsg))
			}
		default:
			goto finish
		}
	}

finish:
	for _, msg := range c.inFlightMessages {
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			log.Errorf("CHANNEL(%s): in-flight-pqueue failed to write message to backend - %s", c.name, err)
		}
	}

	for _, item := range c.deferredMessages {
		msg := item.Value.(*Message)
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			log.Errorf("CHANNEL(%s): defer-pqueue failed to write message to backend - %s", c.name, err)
		}
	}

	log.Infof("CHANNEL(%s): flushing %d memory messages to backend over",
		c.name, len(c.memoryMsgChan))
	return nil
}

func (c *Channel) process() {
	var msg *Message
	var buf []byte
	var err error
	var memoryMsgChan chan *Message
	var backendChan chan []byte
	var handler Handler

	memoryMsgChan = c.memoryMsgChan
	backendChan = c.backend.ReadChan()

	for {
		select {
		case msg = <-memoryMsgChan:
		case buf = <-backendChan:
			msg, err = decodeJson2Message(buf)
			if err != nil {
				log.Errorf("CHANNEL(%s): Message(%s) decode message error.", c.name, string(buf))
				continue
			}
		case pause := <-c.pauseChan:
			if pause {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = c.memoryMsgChan
				backendChan = c.backend.ReadChan()
			}
			continue
		case <-c.exitChan:
			log.Infof("CHANNEL(%s): exit.", c.name)
			c.wg.Done()
			return
		}

		//处理消息,仅仅支持同步调用
		//TODO：有需求后续支持异步调用
		handler = c.GetHandler()
		if handler != nil {
			err = handler.Handle(msg)
			if err == nil {
				c.FinishMessage(msg.ID)
			} else {
				timeout := c.retryTimeout(msg)
				c.RequeueMessage(msg.ID, timeout)
			}
		}
	}
}

func getBackendName(topicName, channelName string) string {
	// backend names, for uniqueness, automatically include the topic... <topic>.<channel>
	backendName := topicName + "." + channelName
	return backendName
}

func (c *Channel) Depth() int64 {
	return int64(len(c.memoryMsgChan)) + c.backend.Depth()
}

func (c *Channel) Pause() error {
	return c.doPause(true)
}

func (c *Channel) UnPause() error {
	return c.doPause(false)
}

func (c *Channel) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&c.paused, 1)
	} else {
		atomic.StoreInt32(&c.paused, 0)
	}

	select {
	case c.pauseChan <- pause:
	case <-c.exitChan:
	}

	return nil
}

func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

// FinishMessage successfully discards an in-flight message
func (c *Channel) FinishMessage(id MessageID) error {
	msg, err := c.popInFlightMessage(id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	return nil
}

// RequeueMessage requeues a message based on `time.Duration`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
//
func (c *Channel) RequeueMessage(id MessageID, timeout time.Duration) error {
	// remove from inflight first
	msg, err := c.popInFlightMessage(id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)

	if timeout == 0 {
		c.exitMutex.RLock()
		err := c.doRequeue(msg)
		c.exitMutex.RUnlock()
		return err
	}

	// deferred requeue
	return c.StartDeferredTimeout(msg, timeout)
}

func (c *Channel) StartInFlightTimeout(msg *Message, timeout time.Duration) error {
	now := time.Now()
	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()
	err := c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

func (c *Channel) StartDeferredTimeout(msg *Message, timeout time.Duration) error {
	absTs := time.Now().Add(timeout).UnixNano()
	item := &pqueue.Item{Value: msg, Priority: absTs}
	err := c.pushDeferredMessage(item)
	if err != nil {
		return err
	}
	c.addToDeferredPQ(item)
	return nil
}

// doRequeue performs the low level operations to requeue a message
//
// Callers of this method need to ensure that a simultaneous exit will not occur
func (c *Channel) doRequeue(m *Message) error {
	err := c.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&c.requeueCount, 1)
	return nil
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
func (c *Channel) pushInFlightMessage(msg *Message) error {
	c.inFlightMutex.Lock()
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		c.inFlightMutex.Unlock()
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[msg.ID] = msg
	c.inFlightMutex.Unlock()
	return nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
func (c *Channel) popInFlightMessage(id MessageID) (*Message, error) {
	c.inFlightMutex.Lock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		return nil, errors.New("ID not in flight")
	}

	delete(c.inFlightMessages, id)
	c.inFlightMutex.Unlock()
	return msg, nil
}

func (c *Channel) addToInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
}

func (c *Channel) removeFromInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	if msg.index == -1 {
		// this item has already been popped off the pqueue
		c.inFlightMutex.Unlock()
		return
	}
	c.inFlightPQ.Remove(msg.index)
	c.inFlightMutex.Unlock()
}

func (c *Channel) pushDeferredMessage(item *pqueue.Item) error {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	id := item.Value.(*Message).ID
	_, ok := c.deferredMessages[id]
	if ok {
		c.deferredMutex.Unlock()
		return errors.New("ID already deferred")
	}
	c.deferredMessages[id] = item
	c.deferredMutex.Unlock()
	return nil
}

func (c *Channel) popDeferredMessage(id MessageID) (*pqueue.Item, error) {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	item, ok := c.deferredMessages[id]
	if !ok {
		c.deferredMutex.Unlock()
		return nil, errors.New("ID not deferred")
	}
	delete(c.deferredMessages, id)
	c.deferredMutex.Unlock()
	return item, nil
}

func (c *Channel) addToDeferredPQ(item *pqueue.Item) {
	c.deferredMutex.Lock()
	heap.Push(&c.deferredPQ, item)
	c.deferredMutex.Unlock()
}

func (c *Channel) processDeferredQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.deferredMutex.Lock()
		item, _ := c.deferredPQ.PeekAndShift(t)
		c.deferredMutex.Unlock()

		if item == nil {
			goto exit
		}
		dirty = true

		msg := item.Value.(*Message)
		_, err := c.popDeferredMessage(msg.ID)
		if err != nil {
			goto exit
		}

		c.doRequeue(msg)
	}

exit:
	return dirty
}

func (c *Channel) processInFlightQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.inFlightMutex.Lock()
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		c.inFlightMutex.Unlock()

		if msg == nil {
			goto exit
		}
		dirty = true

		_, err := c.popInFlightMessage(msg.ID)
		if err != nil {
			goto exit
		}
		atomic.AddUint64(&c.timeoutCount, 1)

		//TODO:maybe we should monitor timeout messages
		c.doRequeue(msg)
	}

exit:
	return dirty
}

func (c *Channel) retryTimeout(msg *Message) time.Duration {
	var retryNano int64
	if c.retryStrategy == RetryStategyEqual {
		retryNano = c.retryIntervalSec * 1000 * 1000 * 1000
	} else if c.retryStrategy == RetryStategyGrow {
		retryNano = int64(msg.Attempts) * c.retryIntervalSec * 1000 * 1000 * 1000
	} else {
		retryNano = c.retryIntervalSec * 1000 * 1000 * 1000
	}
	return time.Duration(retryNano)
}
