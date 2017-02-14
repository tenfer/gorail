package main

import (
	"flag"
	"fmt"
	"net/http"
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

	r.Run()

	//这里先简单测试
	r.AddChannel("test_channel", &rail.File{})
	r.AddChannel("test_channel2", &rail.Http{
		Client:  &http.Client{},
		URL:     "http://127.0.0.1/test.php",
		Timeout: 1000,
	})

	signal := <-sc

	log.Errorf("program terminated! signal:%v", signal)

}
