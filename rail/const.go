package rail

import (
	"errors"
)

var (
	ErrConfigFileNotExisted = errors.New("config file not existed")
)

const (
	FlagInit = iota
	FlagClosing
)
