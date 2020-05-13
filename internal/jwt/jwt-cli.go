package jwt

import (
	"fmt"
        "log"
	"strconv"
	"time"
        
)

type JwtToken struct {
	Token string  `json:"token" description:"token"`
	Iss   string  `json:"iss" description:"iss"`
	Exp   float64 `json:"exp" description:"exp"`
	Key   string  `json:"key" description:"key"`
	Fun   string  `json:"fun" description:"fun"`
}

func NewJwtToken() *JwtToken {
        exp1,_:=strconv.ParseFloat(fmt.Sprintf("%v", time.Now().Unix()+3600*24*30*666), 64)
	njt := &JwtToken{
		Exp: exp1,
		Key: "wifi",
		Fun: "HS256",
                Iss: "wifi",
	}
	return njt
}

func (jt *JwtToken) CreateToken(m map[string]interface{}, keystr string) string {
	claims := map[string]interface{}{
		"iss": jt.Iss,
		"exp": jt.Exp,
	}
	for i, val := range m {
		claims[i] = val
	}
	jt.Key = keystr
	key := []byte(keystr)
	encoded, encodeErr := Encode(
		claims,
		key,
		jt.Fun,
	)
        if encodeErr!=nil {
            fmt.Printf("Failed to encode: ", encodeErr)
        }
	jt.Token = string(encoded)
	return string(encoded)
}

func (jt *JwtToken) VerifyToken(tokenString string, key string) (map[string]interface{}, string) {
	var claimsDecoded map[string]interface{}
	decodeErr := Decode([]byte(tokenString), &claimsDecoded, []byte(key))
	if decodeErr != nil {
                log.Printf("Failed to encode: ",decodeErr)
		return nil, "fail"
	}
	t := time.Now()
        oldT := int64(claimsDecoded["exp"].(float64))
	ct := t.UTC().Unix()
        tokenState := ""
	if ct > oldT {
		tokenState = "fail"
	} else {
		tokenState = "ok"
	}
	return claimsDecoded, tokenState
}

