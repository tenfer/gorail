package rail

import (
	"time"

	"github.com/ngaut/log"
)

type File struct {
}

func NewFile(cc *ChannelConfig) Handler {
	return &File{}
}

func (f *File) Close() error {
	return nil
}

func (f *File) Handle(m *Message) error {
	startTime := time.Now().UnixNano()

	jsonByte, err := m.Encode2Json()

	log.Error(string(jsonByte))
	endTime := time.Now().UnixNano()
	log.Debugf("stdio handle one message spend time: %d", endTime-startTime)
	return err
}
