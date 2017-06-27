package rail

//this package is utility for monitor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ngaut/log"
	"math"
	"time"
)

const (
	OK = iota
	ERR
)
const CountPeriod = 30
const CountChannelBufSize = 10000

type CountItem struct {
	Name     string
	CostTime int64
	Result   int
}

type CountScope struct {
	count   *CountItem
	channel chan *CountItem
}

type CountStat struct {
	MaxCost int64
	MinCost int64
	SumCost int64
	Num     int
}

type CountGlobal struct {
	countStatMap  map[string]map[int]*CountStat
	statStartTime int64
	channel       chan *CountItem
	timerChan     chan bool
}

type CountStatInfo struct {
	Name   string  `json:"name"`
	Status string  `json:"status"`
	Qps    float64 `json:"qps"`
	Count  int     `json:"count"`
	Avg    float64 `json:"avg"`
	Max    int64   `json:"max"`
	Min    int64   `json:"min"`
}

var countGlobal = &CountGlobal{
	make(map[string]map[int]*CountStat),
	GetMicroSecond(),
	make(chan *CountItem, CountChannelBufSize),
	make(chan bool, 5),
}

func init() {
	countGlobal.Start()
}

func GetCountGlobal() *CountGlobal {
	return countGlobal
}

func (cg *CountGlobal) countStat(statMap map[string]map[int]*CountStat, startTime int64, endTime int64) []CountStatInfo {
	costTime := endTime - startTime
	countstat := make([]CountStatInfo, len(statMap)*2)
	indexstat := 0
	for name, resultMap := range statMap {
		for result, stat := range resultMap {
			if result == OK {
				countstat[indexstat].Status = "OK"
			} else if result == ERR {
				countstat[indexstat].Status = "ERR"
			} else {
				continue
				//countInfo.Name = name + ".UNKNOWN"
			}
			countstat[indexstat].Name = name

			countstat[indexstat].Qps = float64(stat.Num) * 1000000 / float64(costTime)
			countstat[indexstat].Count = stat.Num
			countstat[indexstat].Avg = float64(stat.SumCost) / float64(stat.Num)
			countstat[indexstat].Max = stat.MaxCost
			countstat[indexstat].Min = stat.MinCost

			indexstat++
		}
	}
	return countstat[0:indexstat]
}

func (cs *CountScope) SetOk() {
	cs.count.Result = OK
}

func (cs *CountScope) SetErr() {
	cs.count.Result = ERR
}

func (cs *CountScope) End() {
	cs.count.CostTime = GetMicroSecond() - cs.count.CostTime
	select {
	case cs.channel <- cs.count:
	default:
		log.Warn("count channel is full")
	}
}

func (cg *CountGlobal) NewCountScope(name string) *CountScope {
	return &CountScope{&CountItem{name, GetMicroSecond(), OK}, cg.channel}
}

func (cg *CountGlobal) Start() {
	go cg.run()
}

func (cg *CountGlobal) run() {
	go func() {
		for {
			time.Sleep(time.Second * CountPeriod) // sleep one second
			cg.timerChan <- true
		}
	}()

	printCount := 0
	for {
		select {
		case cItem := <-cg.channel:
			cg.AddItem(cItem)
		case ret := <-cg.timerChan:
			if ret {
				printCount++
				if printCount > 1 {
					log.Error("prometheus not get count info")
					cg.timerPrintStat()
				}
			} else {
				printCount = 0
			}
		}
	}
}

func (cg *CountGlobal) printStat(cstatInfo []CountStatInfo) {

	buffer := new(bytes.Buffer)
	timestr := time.Now()
	fmt.Fprint(buffer, "State Info:\n")
	for _, countInfo := range cstatInfo {
		if bs, err := json.Marshal(countInfo); err != nil {
			log.Warn("counter_stat json Marshal fail:" + err.Error())
		} else {
			fmt.Fprintf(buffer, "	%s	[counter_stat] %s\n", timestr, string(bs))
		}
	}

	log.Info(string(buffer.Bytes()))
}

func (cg *CountGlobal) timerPrintStat() {

	go func(cg *CountGlobal) {
		cStat := cg.clearAndGetStat()
		cg.printStat(cStat)
	}(cg)
}

func (cg *CountGlobal) clearAndGetStat() []CountStatInfo {
	stat := cg.countStatMap
	startTime := cg.statStartTime
	endTime := GetMicroSecond()
	cg.countStatMap = make(map[string]map[int]*CountStat)
	cg.statStartTime = GetMicroSecond()

	return cg.countStat(stat, startTime, endTime)
}

func (cg *CountGlobal) GetStat() []CountStatInfo {
	cg.timerChan <- false
	cStat := cg.clearAndGetStat()
	cg.printStat(cStat)
	return cStat
}

func (cg *CountGlobal) AddItem(ci *CountItem) {
	var resultMap map[int]*CountStat
	var ok bool
	resultMap, ok = cg.countStatMap[ci.Name]
	if ok != true {
		resultMap = make(map[int]*CountStat)
		cg.countStatMap[ci.Name] = resultMap
	}

	var countStat *CountStat
	countStat, ok = resultMap[ci.Result]
	if ok != true {
		countStat = &CountStat{0, math.MaxInt64, 0, 0}
		resultMap[ci.Result] = countStat
	}

	countStat.MaxCost = MaxInt64(countStat.MaxCost, ci.CostTime)
	countStat.MinCost = MinInt64(countStat.MinCost, ci.CostTime)
	countStat.SumCost += ci.CostTime
	countStat.Num += 1
}
