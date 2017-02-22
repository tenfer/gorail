package rail

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/ngaut/log"
)

type Result struct {
	Errno  string      `json:"errno"`
	Errmsg string      `json:"errmsg"`
	Data   interface{} `json:"data"`
}

//定义错误码
const (
	ErrSucc                = "0"
	ErrChannelNameEmpty    = "00101"
	ErrChannelExist        = "00102"
	ErrChannelTypeNotExist = "00103"
	ErrChannelNeedHttpUrl  = "00104"
	ErrChannelNeedTcpAddr  = "00105"
	ErrChannelPauseInvalid = "00106"
	ErrChannelNotExist     = "00107"
)

var errMap = map[string]string{
	ErrSucc:                "succ",
	ErrChannelNameEmpty:    "channel name empty",
	ErrChannelExist:        "channel exist",
	ErrChannelTypeNotExist: "channel type not exist",
	ErrChannelNeedHttpUrl:  "channel type is http, httpUrl must not be empty",
	ErrChannelNeedTcpAddr:  "channel type is tcp, tcpAddr must not be empty",
	ErrChannelPauseInvalid: "pause should be 1 or 0",
	ErrChannelNotExist:     "channel not exist",
}

//定义默认值
var (
	defaultRetryStrategy      = RetryStategyGrow
	defaultConnectTimeoutMs   = 1000
	defaultReadWriteTimeoutMs = 2000
	defaultRetryMaxTimes      = 50
	defaultRetryIntervalSec   = 60
	defaultConcurrentNum      = 1
	defaultMsgTimeoutMs       = 5000
)

//配置uri和处理函数
var httpRouterMap = map[string]func(http.ResponseWriter, *http.Request){
	"/channel/add":    channelAdd,
	"/channel/del":    channelDel,
	"/channel/pause":  channelPause,
	"/channel/get":    channelGet,
	"/channel/status": channelStatus,

	"/topic/pause":       topicPause,
	"/topic/getchannels": topicGetChannels,
	"/topic/status":      topicStatus,
}

var channelHandlerMap = map[string]func(*ChannelOption) Handler{
	"http": NewHttp,
}

//GetHandler 返回channel的handler实例
func GetHandlerInstance(option *ChannelOption) Handler {
	if f, ok := channelHandlerMap[option.Ctype]; ok {
		return f(option)
	}
	return nil
}

//keep a global rail instance
var globalRail *Rail
var once sync.Once

func registerHttpHandlers(r *Rail) {
	once.Do(func() {
		for url, function := range httpRouterMap {
			http.HandleFunc(url, function)
		}
		globalRail = r
	})
}

//renderResult 输出json返回值
func renderResult(result *Result) []byte {
	if result.Errmsg == "" {
		if msg, ok := errMap[result.Errno]; ok {
			result.Errmsg = msg
		}
	}
	log.Debugf("result:%v", result)
	b, _ := json.Marshal(&result)
	log.Debugf("json:%s", string(b))
	return b
}

//channelAdd 添加channel
func channelAdd(response http.ResponseWriter, request *http.Request) {
	request.ParseForm()

	var entity ChannelOption = ChannelOption{}
	var result *Result = &Result{}

	entity.Name = request.Form.Get("name")
	if entity.Name == "" {
		result.Errno = ErrChannelNameEmpty
		response.Write(renderResult(result))
		return
	}
	if globalRail.topic.ifExistChannel(entity.Name) {
		result.Errno = ErrChannelExist
		response.Write(renderResult(result))
		return
	}
	entity.Ctype = request.Form.Get("ctype")
	entity.HttpUrl = request.Form.Get("httpUrl")
	entity.TcpAddr = request.Form.Get("tcpAddr")
	if entity.Ctype == "" && entity.HttpUrl != "" {
		entity.Ctype = "http"
	} else if entity.Ctype == "" && entity.TcpAddr != "" {
		entity.Ctype = "tcp"
	} else if _, ok := channelHandlerMap[entity.Ctype]; !ok {
		result.Errno = ErrChannelTypeNotExist
		response.Write(renderResult(result))
		return
	}
	if entity.Ctype == "http" && entity.HttpUrl == "" {
		result.Errno = ErrChannelNeedHttpUrl
		response.Write(renderResult(result))
		return
	}
	if entity.Ctype == "tcp" && entity.TcpAddr == "" {
		result.Errno = ErrChannelNeedTcpAddr
		response.Write(renderResult(result))
		return
	}

	if timeoutMs := request.Form.Get("connectTimeoutMs"); timeoutMs == "" {
		entity.ConnectTimeoutMs = time.Duration(defaultConnectTimeoutMs)
	} else {
		t, _ := strconv.Atoi(timeoutMs)
		entity.ConnectTimeoutMs = time.Duration(t)
	}

	if timeoutMs := request.Form.Get("readWriteTimeoutMs"); timeoutMs == "" {
		entity.ReadWriteTimeoutMs = time.Duration(defaultReadWriteTimeoutMs)
	} else {
		t, _ := strconv.Atoi(timeoutMs)
		entity.ReadWriteTimeoutMs = time.Duration(t)
	}

	if retryMaxTimes := request.Form.Get("retryMaxTimes"); retryMaxTimes == "" {
		entity.RetryMaxTimes = defaultRetryMaxTimes
	} else {
		t, _ := strconv.Atoi(retryMaxTimes)
		entity.RetryMaxTimes = t
	}

	if retryIntervalSec := request.Form.Get("retryIntervalSec"); retryIntervalSec == "" {
		entity.RetryIntervalSec = time.Duration(defaultRetryIntervalSec)
	} else {
		t, _ := strconv.Atoi(retryIntervalSec)
		entity.RetryIntervalSec = time.Duration(t)
	}

	if retryStrategy := request.Form.Get("retryStrategy"); retryStrategy == "" {
		entity.RetryStrategy = defaultRetryStrategy
	} else {
		t, _ := strconv.Atoi(retryStrategy)
		entity.RetryStrategy = t
	}

	if concurrentNum := request.Form.Get("concurrentNum"); concurrentNum == "" {
		entity.ConcurrentNum = defaultConcurrentNum
	} else {
		t, _ := strconv.Atoi(concurrentNum)
		entity.ConcurrentNum = t
	}

	if msgTimeoutMs := request.Form.Get("msgTimeoutMs"); msgTimeoutMs == "" {
		entity.MsgTimeoutMs = time.Duration(defaultMsgTimeoutMs)
	} else {
		t, _ := strconv.Atoi(msgTimeoutMs)
		entity.MsgTimeoutMs = time.Duration(t)
	}

	entity.TopicName = globalRail.topic.name
	globalRail.topic.AddChannel(entity.Name, entity, true)

	result.Errno = ErrSucc
	response.Write(renderResult(result))
}

func channelPause(response http.ResponseWriter, request *http.Request) {
	request.ParseForm()

	var result *Result = &Result{}
	name := request.Form.Get("name")
	if name == "" {
		result.Errno = ErrChannelNameEmpty
		response.Write(renderResult(result))
		return
	}
	pause := request.Form.Get("pause")
	var pauseBool bool
	if pause == "" {
		pauseBool = false
	} else {
		var err error
		pauseBool, err = strconv.ParseBool(pause)
		if err != nil {
			result.Errno = ErrChannelPauseInvalid
			response.Write(renderResult(result))
			return
		}
	}

	channel, ok := globalRail.topic.GetChannel(name)
	if !ok {
		result.Errno = ErrChannelNotExist
		response.Write(renderResult(result))
		return
	}
	if pauseBool {
		channel.Pause()
	} else {
		channel.UnPause()
	}

	result.Errno = ErrSucc
	response.Write(renderResult(result))
}

func channelDel(response http.ResponseWriter, request *http.Request) {
	request.ParseForm()

	var result *Result = &Result{}
	name := request.Form.Get("name")
	if name == "" {
		result.Errno = ErrChannelNameEmpty
		response.Write(renderResult(result))
		return
	}

	_, ok := globalRail.topic.GetChannel(name)
	if !ok {
		result.Errno = ErrChannelNotExist
		response.Write(renderResult(result))
		return
	}

	globalRail.topic.DeleteExistingChannel(name)

	result.Errno = ErrSucc
	response.Write(renderResult(result))
}

func channelGet(response http.ResponseWriter, request *http.Request) {
	request.ParseForm()

	var result *Result = &Result{}
	name := request.Form.Get("name")
	if name == "" {
		result.Errno = ErrChannelNameEmpty
		response.Write(renderResult(result))
		return
	}

	channel, ok := globalRail.topic.GetChannel(name)
	if !ok {
		result.Errno = ErrChannelNotExist
		response.Write(renderResult(result))
		return
	}

	result.Errno = ErrSucc
	result.Data = channel.option
	response.Write(renderResult(result))
}

func channelStatus(response http.ResponseWriter, request *http.Request) {
}

func topicPause(response http.ResponseWriter, request *http.Request) {
	request.ParseForm()

	var result *Result = &Result{}

	pause := request.Form.Get("pause")
	var pauseBool bool
	if pause == "" {
		pauseBool = false
	} else {
		var err error
		pauseBool, err = strconv.ParseBool(pause)
		if err != nil {
			result.Errno = ErrChannelPauseInvalid
			response.Write(renderResult(result))
			return
		}
	}

	if pauseBool {
		globalRail.topic.Pause()
	} else {
		globalRail.topic.UnPause()
	}

	result.Errno = ErrSucc
	response.Write(renderResult(result))
}

func topicGetChannels(response http.ResponseWriter, request *http.Request) {
	var result *Result = &Result{}

	chans := globalRail.channels()

	var channelOptions []*ChannelOption
	for _, c := range chans {
		channelOptions = append(channelOptions, &c.option)
	}

	result.Errno = ErrSucc
	result.Data = channelOptions
	response.Write(renderResult(result))
}
func topicStatus(response http.ResponseWriter, request *http.Request) {
	var result *Result = &Result{}

	result.Errno = ErrSucc
	result.Data = globalRail.topic.option
	response.Write(renderResult(result))
}
