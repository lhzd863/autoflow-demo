package main

import (
	"flag"
        "fmt"
        "os/signal"
        "log"
        "os"

        "github.com/lhzd863/autoflow/internal/util"
        "github.com/lhzd863/autoflow/mst"
)

var (
	showVersion = flag.Bool("version", false, "print version")
	conf        = flag.String("conf", "", "confige file")
        worktype    = flag.String("worktype","","work type")
)

func main() {
        var waitGroup util.WaitGroupWrapper
	flag.Parse()

	if *showVersion {
		fmt.Printf("version v%s\n", util.BINARY_VERSION)
		return
	}

        if *conf=="" {
           log.Fatalf("--conf must be specified")
           return
        }
        exitChan := make(chan int)
        signalChan := make(chan os.Signal, 1)

        go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, os.Interrupt)
 
        waitGroup.Wrap(func() {
             if *worktype == "mst"|| *worktype=="all" {
                m := make(map[string]interface{})
                m["flowid"] = "mst"
                m["apiserverip"] = "127.0.0.1"
                m["apiserverport"] = "12310"
                m["port"] = "12311"
                m["jwtkey"] = "azz"
                nm := mst.NewMst(m)
                nm.Main()
             }
        })


	<-exitChan


	waitGroup.Wait()       
}

