package rail

import (
	"bytes"
	"encoding/gob"
)

func LowerFirstLetter(str string) string {
	if str == "" {
		return str
	}
	if str[0] >= 65 && str[0] <= 90 {
		r := []rune(str)
		r[0] = r[0] + 32
		return string(r)
	} else {
		return str
	}
}
func UpperFirstLetter(str string) string {
	if str == "" {
		return str
	}
	if str[0] >= 97 && str[0] <= 122 {
		r := []rune(str)
		r[0] = r[0] - 32
		return string(r)
	} else {
		return str
	}
}

func DeepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}
