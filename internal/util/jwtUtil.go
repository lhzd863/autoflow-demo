package util

import (
  "log"

  "github.com/lhzd863/tools-server/jwt"
)

func JwtAccessTokenUserName(accesstoken string,jwtkey string) (string,error) {
   var claimsDecoded map[string]interface{}
   decodeErr := jwt.Decode([]byte(accesstoken), &claimsDecoded, []byte(jwtkey))
   if decodeErr != nil {
       log.Printf("Failed to decode: %s (%s)", decodeErr, accesstoken)
       return "",decodeErr
   }
   username:=claimsDecoded["iss"].(string)
   return username,nil
}
