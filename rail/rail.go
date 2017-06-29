package rail

import (
	"bytes"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"time"

	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

const (
	LogTypeSingle = iota
	LogTypeDay
	LogTypeHour
)

type MysqlPos struct {
	Addr string `toml:"addr"`
	Name string `toml:"bin_name"`
	Pos  uint32 `toml:"bin_pos"`
}

//Rail 定义Rail的结构
type Rail struct {
	*canal.DummyEventHandler

	c     *Config
	canal *canal.Canal
	topic *Topic //每个rail设计成只支持单个topic

	httpListener net.Listener

	idChan   chan MessageID
	exitChan chan struct{}

	pos     *MysqlPos
	posLock sync.Mutex

	waitGroup WaitGroupWrapper
	poolSize  int
}

//NewRail 初始化
func NewRail(c *Config) (*Rail, error) {
	//日志目录确保存在
	dir := filepath.Dir(c.LogConfig.Path)
	exist, _ := PathExists(dir)

	if !exist {
		err := os.Mkdir(dir, os.ModePerm)

		if err != nil {
			return nil, err
		}
	}

	//配置日志
	log.SetHighlighting(c.LogConfig.Highlighting)
	log.SetLevel(log.StringToLogLevel(c.LogConfig.Level))
	log.SetFlags(log.Lshortfile | log.Ldate | log.Ltime)
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
	cfg.Dump.ExecutionPath = "" //不支持mysqldump
	cfg.Flavor = c.MysqlConfig.Flavor
	cfg.LogLevel = c.LogConfig.Level

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
		r.canal.SetEventHandler(r)

		pos, err := r.loadMasterInfo()
		if err != nil {
			log.Fatalf("load binlog position error - %s", err)
		}

		//启动canal
		r.canal.StartFrom(*pos)

		//启动http server
		r.waitGroup.Wrap(func() { r.startHttpServer() })

		//启动msg id分配器
		r.waitGroup.Wrap(func() { r.idPump() })

		//定时扫描in-flight和deferred队列
		r.waitGroup.Wrap(func() { r.queueScanLoop() })

		//定时保存binlog position
		r.waitGroup.Wrap(func() { r.saveMasterInfoLoop() })

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

	//save binlog postion
	pos := r.canal.SyncedPosition()
	err := r.saveMasterInfo(pos.Name, pos.Pos)
	if err != nil {
		log.Warnf("save binlog position error when closing - %s", err)
	}

	//关闭topic
	err = r.topic.Close()
	if err != nil {
		log.Errorf("TOPIC(%s): close fail - %s", r.topic.name, err)
	}

	close(r.exitChan)

	r.waitGroup.Wait()

	log.Info("rail safe close.")
}

//onRow 实现接口RowEventHandler,处理binlog事件
func (r *Rail) OnRow(e *canal.RowsEvent) error {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("internal error - %s", err)
		}
	}()

	if r.c.TopicConfig.Schema != "" && e.Table.Schema != r.c.TopicConfig.Schema {
		return nil
	}

	if r.c.TopicConfig.Table != "" {
		regExp, err := regexp.Compile(r.c.TopicConfig.Table)
		//正则表达式出错
		if err != nil {
			log.Errorf("regexp(%s) error - %s", r.c.TopicConfig.Table, err)
			return err
		}
		if !regExp.Match([]byte(e.Table.Name)) {
			return nil
		}
	}

	select {
	case id := <-r.idChan:
		strid := string(id[:])
		msg := NewMessage(strid, e)

		log.Infof("push message(id=%s db=%s table=%s action=%s pk=%s) to topic", msg.ID, msg.Schema, msg.Table, msg.Action, msg.Brief())

		return r.topic.Push(msg)
	}
}

func (r *Rail) OnRotate(e *replication.RotateEvent) error {
	return r.saveMasterInfo(string(e.NextLogName), uint32(e.Position))
}

//String  实现接口RowEventHandler
func (r *Rail) String() string {
	return "rail"
}

func (r *Rail) getMasterInfoPath() string {
	return r.c.BackendConfig.DataPath + "/" + "master.info"
}
func (r *Rail) loadMasterInfo() (*mysql.Position, error) {
	f, err := os.Open(r.getMasterInfoPath())
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	} else if os.IsNotExist(err) {
		//文件不存在,默认从最新的位置开始
		return r.getNewestPos()
	}

	defer f.Close()

	var mysqlPos MysqlPos
	_, err = toml.DecodeReader(f, &mysqlPos)
	if err != nil || mysqlPos.Addr != r.c.MysqlConfig.Addr || mysqlPos.Name == "" {
		return r.getNewestPos()
	}

	return &mysql.Position{mysqlPos.Name, mysqlPos.Pos}, nil
}

//得到最新的binlog位置
func (r *Rail) getNewestPos() (*mysql.Position, error) {
	result, err := r.canal.Execute("SHOW MASTER STATUS")
	if err != nil {
		return nil, fmt.Errorf("show master status error - %s", err)
	}

	if result.Resultset.RowNumber() != 1 {
		return nil, errors.New("select master info error")
	}

	binlogName, _ := result.GetStringByName(0, "File")
	binlogPos, _ := result.GetIntByName(0, "Position")

	log.Infof("fetch mysql(%s)'s the newest pos:(%s, %d)", r.c.MysqlConfig.Addr, binlogName, binlogPos)

	return &mysql.Position{binlogName, uint32(binlogPos)}, nil
}

func (r *Rail) saveMasterInfo(posName string, pos uint32) error {
	r.posLock.Lock()
	defer r.posLock.Unlock()

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)

	if r.pos == nil {
		r.pos = &MysqlPos{
			Addr: r.c.MysqlConfig.Addr,
			Name: posName,
			Pos:  pos,
		}
	} else {
		r.pos.Name = posName
		r.pos.Pos = pos
	}

	e.Encode(r.pos)

	f, err := os.Create(r.getMasterInfoPath())
	if err != nil {
		log.Warnf("create master info file error - %s", err)
		return err
	}
	_, err = f.Write(buf.Bytes())
	if err != nil {
		log.Warnf("save master info to file  error - %s", err)
		return err
	}

	log.Debug("save binlog position succ")
	return nil
}

func (r *Rail) saveMasterInfoLoop() {
	ticker := time.NewTicker(r.c.BinlogFlushMs * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			pos := r.canal.SyncedPosition()
			if r.pos == nil || pos.Name != r.pos.Name || pos.Pos != r.pos.Pos {
				err := r.saveMasterInfo(pos.Name, pos.Pos)
				if err != nil {
					log.Warnf("save binlog position error from per second - %s", err)
				}
			}

		case <-r.exitChan:
			log.Info("save binlog position loop exit.")
			return
		}
	}

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
