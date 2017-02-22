package rail

import (
	"net"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/canal"
)

const (
	LogTypeSingle = iota
	LogTypeDay
	LogTypeHour
)

//Rail 定义Rail的结构
type Rail struct {
	c     *Config
	canal *canal.Canal
	topic *Topic //每个rail设计成只支持单个topic

	httpListener net.Listener

	idChan   chan MessageID
	exitChan chan struct{}

	waitGroup WaitGroupWrapper
	poolSize  int
}

//NewRail 初始化
func NewRail(c *Config) (*Rail, error) {
	//配置日志
	log.SetHighlighting(c.LogConfig.Highlighting)
	log.SetLevel(log.StringToLogLevel(c.LogConfig.Level))
	log.SetOutputByName(c.LogConfig.Path)
	if c.LogConfig.Type == LogTypeDay {
		log.SetRotateByDay()
	} else if c.LogConfig.Type == LogTypeHour {
		log.SetRotateByHour()
	}

	cfg := canal.NewDefaultConfig()

	cfg.Addr = c.MysqlConfig.Addr
	cfg.User = c.MysqlConfig.User
	cfg.Password = c.MysqlConfig.Password

	if canalIns, err := canal.NewCanal(cfg); err != nil {
		log.Fatal(err)
		return nil, err
	} else {
		r := &Rail{
			c:        c,
			canal:    canalIns,
			idChan:   make(chan MessageID, 4096),
			exitChan: make(chan struct{}),
		}

		ctx := &context{}
		ctx.rail = r

		r.topic = NewTopic(c.TopicConfig.Name, ctx)

		//注册RowsEventHandler
		r.canal.RegRowsEventHandler(r)

		//启动canal
		r.canal.Start()

		//启动http server
		r.waitGroup.Wrap(func() { r.startHttpServer() })
		//启动msg id分配器
		r.waitGroup.Wrap(func() { r.idPump() })

		//定时扫描in-flight和deferred队列
		r.waitGroup.Wrap(func() { r.queueScanLoop() })

		log.Info("rail start ok.")
		return r, nil
	}
}

//Close 关闭Rail,释放资源
func (r *Rail) Close() {
	if r.httpListener != nil {
		r.httpListener.Close()
	}

	//关闭canal
	r.canal.Close()
	//关闭topic
	err := r.topic.Close()
	if err != nil {
		log.Errorf("TOPIC(%s): close fail - %s", r.topic.name, err)
	}

	close(r.exitChan)

	r.waitGroup.Wait()

	log.Info("rail safe close.")
}

//Do 实现接口RowEventHandler,处理binlog事件
func (r *Rail) Do(e *canal.RowsEvent) error {
	select {
	case id := <-r.idChan:
		message := NewMessage(id, e)
		log.Debugf("push msg. msg.ID(%v)", id)
		return r.topic.Push(message)
	}
}

//String  实现接口RowEventHandler
func (r *Rail) String() string {
	return "rail"
}

func (r *Rail) idPump() {
	factory := &guidFactory{}
	lastError := time.Unix(0, 0)
	workerID := int64(0)
	for {
		id, err := factory.NewGUID(workerID)
		if err != nil {
			now := time.Now()
			if now.Sub(lastError) > time.Second {
				// only print the error once/second
				log.Errorf("id pump error(%s)", err)
				lastError = now
			}
			runtime.Gosched()
			continue
		}
		select {
		case r.idChan <- id.Hex():
		case <-r.exitChan:
			goto exit
		}
	}

exit:
	log.Infof("ID: closing")
}

func (r *Rail) startHttpServer() {
	log.Info("start http server ...")
	var err error
	if r.httpListener, err = net.Listen("tcp", ":"+strconv.Itoa(r.c.HttpListen)); err != nil {
		log.Fatalf("startHttpServer listen error(%s)", err.Error())
	}

	registerHttpHandlers(r)

	if err = http.Serve(r.httpListener, nil); err != nil {
		log.Errorf("startHttpServer serve error(%s)", err.Error())
	}
}

// queueScanLoop runs in a single goroutine to process in-flight and deferred
// priority queues. It manages a pool of queueScanWorker (configurable max of
// QueueScanWorkerPoolMax (default: 4)) that process channels concurrently.
//
// It copies Redis's probabilistic expiration algorithm: it wakes up every
// QueueScanInterval (default: 100ms) to select a random QueueScanSelectionCount
// (default: 20) channels from a locally cached list (refreshed every
// QueueScanRefreshInterval (default: 5s)).
//
// If either of the queues had work to do the channel is considered "dirty".
//
// If QueueScanDirtyPercent (default: 25%) of the selected channels were dirty,
// the loop continues without sleep.
func (r *Rail) queueScanLoop() {
	workCh := make(chan *Channel, r.c.QueueScanSelectionCount)
	responseCh := make(chan bool, r.c.QueueScanSelectionCount)
	closeCh := make(chan int)

	workTicker := time.NewTicker(r.c.QueueScanIntervalMs * time.Millisecond)
	refreshTicker := time.NewTicker(r.c.QueueScanRefreshIntervalSec * time.Second)

	channels := r.channels()
	r.resizePool(len(channels), workCh, responseCh, closeCh)

	for {
		select {
		case <-workTicker.C:
			if len(channels) == 0 {
				continue
			}
		case <-refreshTicker.C:
			channels = r.channels()
			r.resizePool(len(channels), workCh, responseCh, closeCh)
			continue
		case <-r.exitChan:
			goto exit
		}

		num := r.c.QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}

	loop:
		for _, i := range UniqRands(num, len(channels)) {
			workCh <- channels[i]
		}

		numDirty := 0
		for i := 0; i < num; i++ {
			if <-responseCh {
				numDirty++
			}
		}

		if float64(numDirty)/float64(num) > r.c.QueueScanDirtyPercent {
			goto loop
		}
	}

exit:
	log.Infof("QUEUESCAN: closing")
	close(closeCh)
	workTicker.Stop()
	refreshTicker.Stop()
}

// queueScanWorker receives work (in the form of a channel) from queueScanLoop
// and processes the deferred and in-flight queues
func (r *Rail) queueScanWorker(workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	for {
		select {
		case c := <-workCh:
			now := time.Now().UnixNano()
			dirty := false
			// if c.processInFlightQueue(now) {
			// 	dirty = true
			// }
			if c.processDeferredQueue(now) {
				dirty = true
			}
			responseCh <- dirty
		case <-closeCh:
			return
		}
	}
}

// resizePool adjusts the size of the pool of queueScanWorker goroutines
//
// 	1 <= pool <= min(num * 0.25, QueueScanWorkerPoolMax)
//
func (r *Rail) resizePool(num int, workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	idealPoolSize := int(float64(num) * 0.25)
	if idealPoolSize < 1 {
		idealPoolSize = 1
	} else if idealPoolSize > r.c.QueueScanWorkerPoolMax {
		idealPoolSize = r.c.QueueScanWorkerPoolMax
	}

	for {
		if idealPoolSize == r.poolSize {
			break
		} else if idealPoolSize < r.poolSize {
			// contract
			closeCh <- 1
			r.poolSize--
			log.Debugf("resize pool minus: size(%d)", r.poolSize)
		} else {
			// expand
			r.waitGroup.Wrap(func() {
				r.queueScanWorker(workCh, responseCh, closeCh)
			})
			r.poolSize++
			log.Debugf("resize pool add: size(%d)", r.poolSize)
		}
	}
}

// channels returns a flat slice of all channels in all topics
func (r *Rail) channels() []*Channel {
	var channels []*Channel

	r.topic.RLock()
	for _, c := range r.topic.channelMap {
		channels = append(channels, c)
	}
	r.topic.RUnlock()
	return channels
}
