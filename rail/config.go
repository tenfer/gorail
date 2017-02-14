package rail

import (
	"io/ioutil"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

const (
	RunModeSync  = iota //同步模式
	RunModeAsync        //异步模式
)

type Config struct {
	RunMode       int            `toml:"run_mode"`
	Profiler      string         `toml:"profiler"`
	BackendConfig *BackendConfig `toml:"backend"`
	LogConfig     *LogConfig     `toml:"log"`
	MysqlConfig   *MysqlConfig   `toml:"mysql"`
	TopicConfig   *TopicConfig   `toml:"topic"`
}

type BackendConfig struct {
	DataPath        string        `toml:"data_path"`
	MaxBytesPerFile int64         `toml:"max_bytes_per_file"`
	MaxMsgSize      int32         `toml:"max_msg_size"`
	SyncEvery       int64         `toml:"sync_every"`
	SyncTimeout     time.Duration `toml:"sync_timeout"`
}

type LogConfig struct {
	Path         string `toml:"path"`
	Type         int    `toml:"type"`
	Highlighting bool   `toml:"highlighting"`
	Level        string `toml:"level"`
}

type MysqlConfig struct {
	Addr     string `toml:"addr"`
	User     string `toml:"user"`
	Password string `toml:"password"`
}
type TopicConfig struct {
	Name           string           `toml:"name"`
	ConcurrentNum  int              `toml:"concurrent_num"`
	MemBuffSize    uint64           `toml:"mem_buff_size"`
	ChannelConfigs []*ChannelConfig `toml:"channels"`
}

type ChannelConfig struct {
	//common
	Name             string `toml:"name"`
	Type             string `toml:"type"`
	ConcurrentNum    int    `toml:"concurrent_num"`
	MemBuffSize      uint64 `toml:"mem_buff_size"`
	RetryMemBuffSize uint64 `toml:"retry_mem_buff_size"`
	RetryMaxTimes    uint16 `toml:"retry_max_times"`
	RetryInternalSec int64  `toml:"retry_internal_sec"`
	//file
	FileDir  string `toml:"file_dir"`
	FileType int    `toml:"file_type"`
	//http
	HttpUrl       string `toml:"http_url"`
	HttpTimeoutMs int64  `toml:"http_timeout_ms"`
}

//NewConfigWithFile 读取配置文件
func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

//NewConfig 解析配置文件
func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &c, nil
}
