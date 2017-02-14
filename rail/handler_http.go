package rail

import (
	"io/ioutil"
	"net/http"
	"time"

	"github.com/ngaut/log"
)

type Http struct {
	Client     *http.Client
	URL        string
	Timeout    time.Duration
	RetryTimes int
}

func NewHttp(cc *ChannelConfig) Handler {
	ht := &Http{Client: &http.Client{}}
	ht.URL = cc.HttpUrl
	ht.Timeout = time.Millisecond * time.Duration(cc.HttpTimeoutMs)
	return ht
}

func (h *Http) Close() error {
	return nil
}

func (h *Http) Handle(m *Message) error {
	startTime := time.Now().UnixNano()

	body, _ := m.Encode2IOReader()
	req, _ := http.NewRequest("POST", h.URL, body)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded;charset=utf-8")

	if resp, err := h.Client.Do(req); err != nil {
		//需要重试
		//TODO
		return err
	} else {
		_, err := ioutil.ReadAll(resp.Body)
		//定义好接口规范，出错是否需要重试
		//TODO
		if err != nil {
			return err
		}

		endTime := time.Now().UnixNano()
		log.Infof("http handle msgID(%v) cost(%d)", m.ID, endTime-startTime)
		return nil
	}
}
