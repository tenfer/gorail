package rail

import (
	"net"
	"runtime"
	"time"

	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/canal"
)

const (
	StatusStop = iota
	StatusRun
)

const (
	LogTypeSingle = iota
	LogTypeDay
	LogTypeHour
)

//Rail 定义Rail的结构
type Rail struct {
	c      *Config
	canal  *canal.Canal
	status int    //状态 0 停止 1 运行
	topic  *Topic //每个rail设计成只支持单个topic

	httpListener net.Listener

	idChan   chan MessageID
	exitChan chan struct{}
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
			status:   StatusStop,
		}

		ctx := &context{}
		ctx.rail = r

		r.topic = NewTopic(c.TopicConfig.Name, ctx)

		//注册RowsEventHandler
		r.canal.RegRowsEventHandler(r)
		return r, nil
	}
}

//Run 启动rail
func (r *Rail) Run() {
	if r.status == StatusStop {
		go r.idPump()

		r.canal.Start()

		r.topic.Run()

		r.status = StatusRun
		log.Info("rail start ok.")
	} else {
		log.Info("rail is already running.")
	}
}

//Close 关闭Rail,释放资源
func (r *Rail) Close() {
	//关闭canal
	r.canal.Close()
	//关闭topic
	r.topic.Close()

	close(r.exitChan)

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

func (r *Rail) AddChannel(channelName string, handler Handler) {
	channel, isNew := r.topic.GetChannel(channelName)
	if isNew {
		channel.AddHandler(handler)
		channel.Run()
	}

	log.Debug("add channel")
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
				log.Errorf("%s", err)
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
