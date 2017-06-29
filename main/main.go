package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	//_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/tenfer/gorail/rail"
)

var (
	configFile = flag.String("config", "./etc/rail.toml", "go-rail config file")
	binlogName = flag.String("binlog_name", "", "binlog file name")
	binlogPos  = flag.Int64("binlog_pos", 0, "binlog position")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var err error
	var config *rail.Config
	config, err = rail.NewConfigWithFile(*configFile)
	if err != nil {
		log.Fatalf("config load failed.detail=%s", errors.ErrorStack(err))
	}

	r, err := rail.NewRail(config)
	defer r.Close()

	if err != nil {
		fmt.Println("new Rail error.", err)
		log.Fatalf("new Rail error. detail:%v", err)
	}

	promeExport := rail.NewExporter(config.HttpListen+1, "gorail", r)
	err = promeExport.Start()

	if err != nil {
		fmt.Printf("promeExport start error:%s\n", err.Error())
		log.Errorf("promeExport start error:%s", err.Error())
		return
	}

	fmt.Println("rail start succ.")

	signal := <-sc

	log.Errorf("program terminated! signal:%v", signal)

}
