package main

import (
  "fmt"
  "crypto/base64"
)

func main(){
    str:="c2Rsc2xkc2QgCmRsc2RzCmxz"
    st:=coder.DecodeString(str)
    fmt.Println(st)

}
