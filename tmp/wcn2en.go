package main
import (
 "github.com/lhzd863/tools-server/db"
 "github.com/lhzd863/tools-server/module"
 "log"
 "strings"
 "os"
 "encoding/json"
 "io"
 "bufio"
)

func main(){
  f, err := os.Open("09003.txt")
  if err != nil {
     log.Println(err)
  }
  defer f.Close()
 
  rd := bufio.NewReader(f)
  bt := db.NewBoltDB("/home/k8s/Go/ws/src/github.com/lhzd863/tools-server/tmp","cn2en")
  defer bt.Close()
  for{
     line, err := rd.ReadString('\n')
     if err != nil || io.EOF == err {
       break
     } 
     arr:=strings.Split(strings.Replace(line,"\n","",-1),",")
     if len(arr) <3 {
        break
     }
     
     jsonstr,_ := json.Marshal(module.Cn2EnFieldBean{CNName:arr[2],ENName:arr[0],Type:arr[1],Tage:"金融",Cts:"2020-01-06 19:00:00",Contributor:"admin"})
     log.Println(string(jsonstr))
     bt.Set(arr[0]+","+arr[2],string(jsonstr))
  }
}
