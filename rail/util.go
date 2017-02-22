package rail

import (
	"bytes"
	"encoding/gob"
	"math/rand"
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

//DeepCopy 复制一份数据
//dst 是结构体的地址 src是结构体
//example:
// type TestStruct struct {
// 	Name string
// 	Age  int
// }

// func main() {
// 	var m2 TestStruct
// 	m1 := &TestStruct{Name: "name", Age: 1}

// 	err := rail.DeepCopy(&m2, *m1)
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	m1.Name = "test"
// 	fmt.Println(m1)
// 	fmt.Println(m2)
// }
func DeepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

func UniqRands(l int, n int) []int {
	set := make(map[int]struct{})
	nums := make([]int, 0, l)
	for {
		num := rand.Intn(n)
		if _, ok := set[num]; !ok {
			set[num] = struct{}{}
			nums = append(nums, num)
		}
		if len(nums) == l {
			goto exit
		}
	}
exit:
	return nums
}
