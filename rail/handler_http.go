package rail

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/ngaut/log"
)

type Http struct {
	client *http.Client
	url    string
	option *ChannelOption
}

func NewHttp(option *ChannelOption) Handler {
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   option.ConnectTimeoutMs * time.Millisecond,
				KeepAlive: option.ConnectTimeoutMs * time.Millisecond,
			}).Dial,
			TLSHandshakeTimeout: option.ConnectTimeoutMs * time.Millisecond,
		},
		Timeout: (option.ConnectTimeoutMs + option.ReadWriteTimeoutMs) * time.Millisecond,
	}

	ht := &Http{client: client}
	ht.url = option.HttpUrl
	ht.option = option

	return ht
}

func (h *Http) Close() error {
	return nil
}

func (h *Http) Handle(m *Message) error {
	startTime := time.Now().UnixNano()
	//添加监控
	countScope := GetCountGlobal().NewCountScope(fmt.Sprintf("downstream_%s", h.option.Name))
	countScope.SetErr()

	defer countScope.End()

	body, _ := m.Encode2IOReader()
	req, _ := http.NewRequest("POST", h.url, body)
	req.Header.Add("Content-Type", "application/json;charset=utf-8")

	if resp, err := h.client.Do(req); err != nil {
		endTime := time.Now().UnixNano()
		log.Errorf("CHANNEL(%s): msgID(%s) - request err(%v), cost(%d us)", h.option.Name, m.ID, err, (endTime-startTime)/1000)
		return err
	} else {
		defer resp.Body.Close()

		var res Result
		var err error
		var b []byte
		b, err = ioutil.ReadAll(resp.Body)
		//定义好接口规范，出错是否需要重试
		if err != nil {
			log.Errorf("CHANNEL(%s): msgID(%s) - read err(%v)", h.option.Name, m.ID, err)
			return err
		}
		err = json.Unmarshal(b, &res)
		if err != nil {
			log.Errorf("CHANNEL(%s): msgID(%s) - response is invalid json string, err(%v)", h.option.Name, m.ID, err)
			return err
		}

		if res.Errno != ErrSucc {
			log.Errorf("CHANNEL(%s): msgID(%s) - get response(%v) error", h.option.Name, m.ID, res)
			return errors.New(res.Errmsg)
		}

		endTime := time.Now().UnixNano()

		countScope.SetOk()

		log.Infof("CHANNEL(%s): msgID(%v) - handled cost(%d us)", h.option.Name, m.ID, (endTime-startTime)/1000)
		return nil
	}
}
