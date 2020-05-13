package glog

import (
   "log"
   "os"
   "time"
   "regexp"

   "github.com/lhzd863/autoflow/internal/util"
)

func Glog(f string,context string) {
        //timeStr := time.Now().Format("2006-01-02 15:04:05")
        timeStr := time.Now().Format("20060102")
        dt:="\\$\\{"+util.ENV_VAR_DATE+"\\}"
        reg := regexp.MustCompile(dt)
        f1:=reg.ReplaceAllString(f,timeStr)
        logFile, err := os.OpenFile(f1, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
        if err != nil {
                panic(err)
        }
        log.SetOutput(logFile)
        log.SetPrefix("[j]")
        //log.SetFlags(log.LstdFlags | log.Lshortfile | log.LUTC)
        log.SetFlags(log.LstdFlags | log.Ltime)
        log.Println(context)
        return
}

