package rail

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/log"
)

type Topic struct {
	sync.RWMutex

	ctx *context

	name          string
	memoryMsgChan chan *Message
	backend       BackendQueue
	concurrentNum int //处理数据并发数

	lastPushNum       uint64
	lastConsumeNum    uint64 //上一次统计的消费数量
	pushNum           uint64 //推送的数量
	consumeNum        uint64 //消费数量
	channelMap        map[string]*Channel
	wg                sync.WaitGroup
	exitChan          chan struct{}
	closing           int32 //topic是否关闭
	channelUpdateChan chan int
	paused            int32
	pauseChan         chan bool
}

func NewTopic(topicName string, ctx *context) *Topic {
	t := &Topic{
		pushNum:           0,
		consumeNum:        0,
		lastConsumeNum:    0,
		lastPushNum:       0,
		name:              topicName,
		ctx:               ctx,
		channelMap:        make(map[string]*Channel),
		exitChan:          make(chan struct{}),
		concurrentNum:     ctx.rail.c.TopicConfig.ConcurrentNum,
		pauseChan:         make(chan bool),
		channelUpdateChan: make(chan int),
		//memoryMsgChan:     make(chan *Message, ctx.rail.c.TopicConfig.MemBuffSize),
		memoryMsgChan: nil,
	}

	t.backend = newDiskQueue(t.name,
		ctx.rail.c.BackendConfig.DataPath,
		ctx.rail.c.BackendConfig.MaxBytesPerFile,
		1,
		ctx.rail.c.BackendConfig.MaxMsgSize,
		ctx.rail.c.BackendConfig.SyncEvery,
		ctx.rail.c.BackendConfig.SyncTimeout,
	)

	return t
}

//Push send one message to topic
func (t *Topic) Push(m *Message) error {
	t.RLock()
	defer t.RUnlock()

	if atomic.LoadInt32(&t.closing) == FlagClosing {
		return errors.New("topic exiting.")
	}

	var err error
	select {
	case t.memoryMsgChan <- m:
	default:
		log.Debugf("disk queue:======================%v", m.ID)
		buf := bufferPoolGet()
		err = writeMessageToBackend(buf, m, t.backend)
		bufferPoolPut(buf)
	}

	if err != nil {
		b, _ := m.Encode2Json()
		log.Errorf("topic(%s) Message(%s) send to Backend Queue error.", t.name, string(b))
		return err
	}

	atomic.AddUint64(&t.pushNum, 1)

	return nil
}

//Run 启动topic以及注册的channels
func (t *Topic) Run() {
	for i := 0; i < t.concurrentNum; i++ {
		t.wg.Add(1)
		go t.process(i)
		log.Infof("consumer's rontine-%d started", i)
	}

	//打印qps信息
	go t.Info()
}

//不断的消费队列中的消息
func (t *Topic) process(taskID int) {
	var msg *Message
	var buf []byte
	var err error
	var chans []*Channel
	var memoryMsgChan chan *Message
	var backendChan chan []byte

	t.RLock()
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()

	if len(chans) > 0 {
		memoryMsgChan = t.memoryMsgChan
		backendChan = t.backend.ReadChan()
	}

	for {
		select {
		case msg = <-memoryMsgChan:
		case buf = <-backendChan:
			msg, err = decodeJson2Message(buf)
			if err != nil {
				log.Errorf("topic(%s) Message(%s) decode message error.", t.name, string(buf))
				continue
			}
		case <-t.channelUpdateChan:
			//channel有更新
			chans = chans[:0]
			t.RLock()
			for _, c := range t.channelMap {
				chans = append(chans, c)
			}
			t.RUnlock()
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case pause := <-t.pauseChan:
			if pause || len(chans) == 0 {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.exitChan:
			log.Infof("topic(%s) taskId(%d) exit.", t.name, taskID)
			t.wg.Done()
			return
		}
		log.Debugf("msg(%v) dispatch to channel", msg.ID)
		for _, channel := range chans {
			chanMsg := msg

			// copy the message because each channel
			// needs a unique instance but...
			// fastpath to avoid copy if its the first channel
			// (the topic already created the first copy)
			//if i > 0 {

			//err := DeepCopy(chanMsg, msg)
			b, _ := chanMsg.Encode2Json()
			log.Debugf("deep copy. chanMsg(%s)", string(b))
			if err != nil {
				bmsg, _ := msg.Encode2Json()
				log.Errorf("topic(%s) failed to copy msg(%s) to channel(%s) - %s", t.name, string(bmsg), channel.name, err)
				continue
			}
			//}
			err = channel.Push(chanMsg)
			if err != nil {
				bmsg, _ := msg.Encode2Json()
				log.Errorf("topic(%s) failed to put msg(%s) to channel(%s) - %s", t.name, string(bmsg), channel.name, err)
				continue
			}
		}

		atomic.AddUint64(&t.consumeNum, 1)
	}
}

func (t *Topic) Close() error {
	if !atomic.CompareAndSwapInt32(&t.closing, FlagClosing, FlagInit) {
		return errors.New("exiting")
	}

	//notify exit
	close(t.exitChan)
	t.wg.Wait()

	for _, channel := range t.channelMap {
		channel.Close()
	}

	t.flush()

	err := t.backend.Close()

	log.Infof("topic(%s) close.", t.name)
	return err
}

func (t *Topic) flush() error {
	var msgBuf bytes.Buffer

	if len(t.memoryMsgChan) > 0 {
		log.Infof("TOPIC(%s): flushing %d memory messages to backend",
			t.name, len(t.memoryMsgChan))
	}
	for {
		select {
		case msg := <-t.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, t.backend)
			if err != nil {
				bmsg, _ := msg.Encode2Json()
				log.Errorf("topic(%s) flush msg (%s) error.", t.name, string(bmsg))
			}
		default:
			goto finish
		}
	}
finish:
	log.Infof("TOPIC(%s): flushing %d memory messages to backend over",
		t.name, len(t.memoryMsgChan))
	return nil
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
func (t *Topic) GetChannel(channelName string) (*Channel, bool) {
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()

	if isNew {
		// update messagePump state
		select {
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}

	return channel, isNew
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		channel = NewChannel(t.name, channelName, t.ctx, deleteCallback)
		t.channelMap[channelName] = channel
		log.Infof("TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	return channel, false
}

// DeleteExistingChannel removes a channel from the topic only if it exists
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		t.Unlock()
		return errors.New("channel does not exist")
	}
	delete(t.channelMap, channelName)
	t.Unlock()

	log.Infof("TOPIC(%s): deleting channel %s", t.name, channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	channel.Delete()

	// update messagePump state
	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}

	return nil
}

func (t *Topic) Pause() error {
	return t.doPause(true)
}

func (t *Topic) UnPause() error {
	return t.doPause(false)
}

func (t *Topic) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&t.paused, 1)
	} else {
		atomic.StoreInt32(&t.paused, 0)
	}

	select {
	case t.pauseChan <- pause:
	case <-t.exitChan:
	}

	return nil
}

func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.paused) == 1
}

//info 输出当前的写入和读出的qps值
func (t *Topic) Info() {
	for {
		select {
		case <-time.After(time.Second):

			pushNum := atomic.LoadUint64(&t.pushNum)
			lastPushNum := atomic.LoadUint64(&t.lastConsumeNum)
			consumeNum := atomic.LoadUint64(&t.consumeNum)
			lastConsumeNum := atomic.LoadUint64(&t.lastConsumeNum)

			pushQps := pushNum - lastPushNum
			consumeQps := consumeNum - lastConsumeNum

			t.lastPushNum = t.pushNum
			t.lastConsumeNum = t.consumeNum

			fmt.Printf("%s: push_num(%d) consume_num(%d) send_qps(%d) consume_qps(%d) mem_num(%d) \n", time.Now(), t.pushNum, t.consumeNum, pushQps, consumeQps, len(t.memoryMsgChan))

			for _, channel := range t.channelMap {
				fmt.Printf("%s: topic(%s) channel(%s) message_count(%d) timeout_count(%d) requeue_count(%d)\n", time.Now(), t.name, channel.name, channel.messageCount, channel.timeoutCount, channel.requeueCount)
			}

			fmt.Println()
		}
	}
}
