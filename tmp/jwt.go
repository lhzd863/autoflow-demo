package main

import (
  "fmt"
  "time"
  "strconv"
  "github.com/lhzd863/yangtze/jwt"
)

func main(){
  exp1,_:=strconv.ParseFloat(fmt.Sprintf("%v",time.Now().Unix()+3600*24*30*666),64)
  //exp1:=strconv.ParseInt(fmt.Sprintf("%v.0",exptime), 10, 64)
  claims := map[string]interface{}{
    "iss":"azz",
    "exp": exp1,
  }
  key := []byte("azz")
  encoded, encodeErr := jwt.Encode(
    claims,
    key,
    "HS256",
  )
  
  if encodeErr != nil {
    fmt.Printf("Failed to encode: ", encodeErr)
  }
  
  fmt.Println(string(encoded))
  var claimsDecoded map[string]interface{}
  decodeErr := jwt.Decode(encoded, &claimsDecoded, key)
  if decodeErr != nil {
    fmt.Printf("Failed to decode: %s (%s)", decodeErr, encoded)
  }
  for k, v := range claims {
    if claimsDecoded[k] != v {
      fmt.Printf("Claim entry '%s' failed: %s != %s", k, claimsDecoded[k], v)
    }
    fmt.Println(k)
    fmt.Println(v)
  }

}
