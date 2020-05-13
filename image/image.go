package main

import (
   "time"

   "github.com/lhzd863/autoflow/internal/glog"
)

func main(){
  logf := "/home/k8s/Go/ws/src/github.com/lhzd863/autoflow/image/tmp.log"
  for {
      glog.Glog(logf, "start ...")
      time.Sleep(10 * time.Second)
  }

}
