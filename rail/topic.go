package rail

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
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

	channelMap        map[string]*Channel
	wg                WaitGroupWrapper
	exitChan          chan struct{}
	closing           int32 //topic是否关闭
	channelUpdateChan chan int
	pauseChan         chan bool

	option TopicOption
}

func NewTopic(topicName string, ctx *context) *Topic {
	t := &Topic{
		name:              topicName,
		ctx:               ctx,
		channelMap:        make(map[string]*Channel),
		exitChan:          make(chan struct{}),
		pauseChan:         make(chan bool),
		channelUpdateChan: make(chan int),
		memoryMsgChan:     make(chan *Message, ctx.rail.c.TopicConfig.MemBuffSize),
		option:            TopicOption{},
	}

	t.backend = newDiskQueue(t.name,
		ctx.rail.c.BackendConfig.DataPath,
		ctx.rail.c.BackendConfig.MaxBytesPerFile,
		1,
		ctx.rail.c.BackendConfig.MaxMsgSize,
		ctx.rail.c.BackendConfig.SyncEvery,
		ctx.rail.c.BackendConfig.SyncTimeout,
	)

	err := t.retrieveMetaData()

	if err != nil && !os.IsNotExist(err) {
		log.Errorf("TOPIC(%s): failed to retrieveMetaData - %s", t.name, err)
	}

	t.run()

	return t
}

//Push send one message to topic
func (t *Topic) Push(m *Message) error {
	t.RLock()
	defer t.RUnlock()

	if atomic.LoadInt32(&t.closing) == FlagClosing {
		return errors.New("topic exiting.")
	}

	//添加监控
	countScope := GetCountGlobal().NewCountScope(fmt.Sprintf("topic_%s_send", t.name))
	countScope.SetErr()
	defer countScope.End()

	var err error
	select {
	case t.memoryMsgChan <- m:
	default:
		buf := bufferPoolGet()
		err = writeMessageToBackend(buf, m, t.backend)
		bufferPoolPut(buf)
	}

	if err != nil {
		b, _ := m.Encode2Json()
		log.Errorf("topic(%s) Message(%s) send to Backend Queue error.", t.name, string(b))
		return err
	} else {
		countScope.SetOk()
	}

	atomic.AddUint64(&t.option.MessageCount, 1)

	return nil
}

//自启动
func (t *Topic) run() {
	t.wg.Wrap(func() { t.process() })

	//change to promtheus
	//打印qps信息
	//go t.Info()
}

//不断的消费队列中的消息
func (t *Topic) process() {
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

	if len(chans) > 0 && !t.IsPaused() {
		memoryMsgChan = t.memoryMsgChan
		backendChan = t.backend.ReadChan()
	} else {
		memoryMsgChan = nil
		backendChan = nil
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

			log.Infof("channel update ....... ,chans(%d)", len(chans))

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
			log.Infof("topic(%s) get exit signal.", t.name)
			return
		}

		//添加监控
		countScope := GetCountGlobal().NewCountScope(fmt.Sprintf("topic_%s_process", t.name))
		countScope.SetErr()

		//log.Debugf("channel ....... ,chans(%d)", len(chans))

		for i, channel := range chans {
			var chanMsg Message
			// copy the message because each channel
			// needs a unique instance but...
			// fastpath to avoid copy if its the first channel
			// (the topic already created the first copy)
			if i > 0 {
				err := DeepCopy(&chanMsg, msg)
				log.Debugf("topic(%s): copy msg.id(%v) to channel(%s)", t.name, chanMsg.ID, channel.name)
				if err != nil {
					bmsg, _ := msg.Encode2Json()
					log.Errorf("topic(%s): failed to copy msg(%s) to channel(%s) - %s", t.name, string(bmsg), channel.name, err)
					continue
				}
			} else {
				chanMsg = *msg
			}

			err = channel.Push(&chanMsg)
			if err != nil {
				bmsg, _ := msg.Encode2Json()
				log.Errorf("topic(%s): failed to send msg(%s) to channel(%s) - %s", t.name, string(bmsg), channel.name, err)
				continue
			}
		}

		atomic.AddUint64(&t.option.MessageFinshCount, 1)
		if err == nil {
			countScope.SetOk()
		}

		countScope.End()
	}
}

func (t *Topic) Close() error {
	if !atomic.CompareAndSwapInt32(&t.closing, FlagInit, FlagClosing) {
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
	if err != nil {
		log.Errorf("TOPIC(%s): backend close fail - %s", t.name, err)
	}

	err = t.persistMetaData()
	if err != nil {
		log.Errorf("TOPIC(%s): persistMetaData fail - %s", t.name, err)
	}

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

func (t *Topic) ifExistChannel(channelName string) bool {
	t.RLock()
	defer t.RUnlock()
	_, ok := t.channelMap[channelName]
	return ok
}

func (t *Topic) GetChannel(channelName string) (*Channel, bool) {
	t.RLock()
	channel, ok := t.channelMap[channelName]
	t.RUnlock()
	return channel, ok
}

// AddChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
func (t *Topic) AddChannel(channelName string, option ChannelOption, notifyChannelUpdate bool) *Channel {
	if channelName == "" {
		return nil
	}

	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName, option)
	t.Unlock()

	if isNew && notifyChannelUpdate {
		// update messagePump state
		select {
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}

		log.Infof("add channel(%s) option(%v)", option.Name, option)
	}

	return channel
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string, option ChannelOption) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		channel = NewChannel(t.name, channelName, option, t.ctx, deleteCallback)
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
		atomic.StoreInt32(&t.option.Paused, 1)
	} else {
		atomic.StoreInt32(&t.option.Paused, 0)
	}

	select {
	case t.pauseChan <- pause:
	case <-t.exitChan:
	}

	return nil
}

func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.option.Paused) == 1
}

func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}

func (t *Topic) retrieveMetaData() error {
	var f *os.File
	var err error
	var b []byte
	var allMetaData TopicMetaData

	fileName := t.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	b, err = ioutil.ReadAll(f)

	if err != nil {
		return err
	}

	err = json.Unmarshal(b, &allMetaData)

	if err != nil {
		return err
	}

	t.option = allMetaData.Toption

	//添加保存的channel
	for _, cmd := range allMetaData.Cmds {
		t.AddChannel(cmd.Name, cmd.ChannelOption, false)
	}

	return nil
}

func (t *Topic) persistMetaData() error {
	var f *os.File
	var b []byte
	var err error
	var chans []*Channel

	t.RLock()
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()

	if len(chans) == 0 {
		return nil
	}

	log.Infof("TOPIC(%s):persistMetaData channels(%d)", t.name, len(chans))

	allMetaData := NewTopicMetaData(t, chans)

	b, err = json.Marshal(allMetaData)

	if err != nil {
		return err
	}

	fileName := t.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(b)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	return os.Rename(tmpFileName, fileName)
}

func (t *Topic) metaDataFileName() string {
	return fmt.Sprintf(path.Join(t.ctx.rail.c.BackendConfig.DataPath, "%s.meta.dat"), t.name)
}

//info 输出当前的写入和读出的qps值
func (t *Topic) Info() {
	for {
		select {
		case <-time.After(5 * time.Second):
			fmt.Printf("%s: message_count(%d) message_finish_count(%d) mem_num(%d) buffered_num(%d) \n", time.Now(), t.option.MessageCount, t.option.MessageFinshCount, len(t.memoryMsgChan), t.Depth())

			for _, channel := range t.channelMap {
				fmt.Printf("%s: topic(%s) channel(%s) message_count(%d) message_finish_count(%d) timeout_count(%d) requeue_count(%d) mem_num(%d) buffered_num(%d) \n", time.Now(), t.name, channel.name, channel.option.MessageCount, channel.option.MessageFinshCount, channel.option.TimeoutCount, channel.option.RequeueCount, len(channel.memoryMsgChan), channel.Depth())
			}

			fmt.Println()
		}
	}
}
