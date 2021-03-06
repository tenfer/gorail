package rail

import "time"

type ChannelOption struct {
	Name               string
	Ctype              string
	RetryStrategy      int
	RetryMaxTimes      int
	RetryIntervalSec   time.Duration
	HttpUrl            string
	TcpAddr            string
	ConnectTimeoutMs   time.Duration
	ReadWriteTimeoutMs time.Duration
	ConcurrentNum      int
	MsgTimeoutMs       time.Duration

	TopicName         string
	RequeueCount      uint64
	MessageCount      uint64
	MessageFinshCount uint64
	TimeoutCount      uint64
	Paused            int32

	Filter *Filter
}

type Filter struct {
	Schemas    []string `json:"schemas"`    //关心的db
	Tables     []string `json:"tables"`     //关心的table
	Actions    []string `json:"actions"`    //action:insert update delete etc.
	Expression string   `json:"expression"` //符合表达式的记录才会推送
}

type TopicOption struct {
	MessageCount      uint64
	MessageFinshCount uint64
	Paused            int32
}

//持久化对象
type ChannelMetaData struct {
	ChannelOption
}

func NewChannelMetaData(c *Channel) ChannelMetaData {
	cma := ChannelMetaData{}
	cma.ChannelOption = c.option
	return cma
}

type TopicMetaData struct {
	Cmds    []ChannelMetaData
	Toption TopicOption
}

func NewTopicMetaData(topic *Topic, chans []*Channel) TopicMetaData {
	var cmds []ChannelMetaData
	for _, c := range chans {
		cmds = append(cmds, NewChannelMetaData(c))
	}

	tmd := TopicMetaData{
		Cmds:    cmds,
		Toption: topic.option,
	}

	return tmd
}
