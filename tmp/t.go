package main

import (
	"bytes"
	"crypto/cipher"
	"crypto/aes"
        "encoding/hex"
	"fmt"
)

func main() {
    str,_:=hex.DecodeString( "bc73a13867952fd5ae7363d9f2c65431")
    st,_:=Decrypt([]byte(str),[]byte("beijingtiananmen"))
    fmt.Println(st)
}

func Decrypt(ciphertext, key []byte) ([]byte, error) {           
        pkey := PaddingLeft(key, '0', 16)                            
        block, err := aes.NewCipher(pkey) //选择加密算法         
        if err != nil {                                          
                return nil, err                                  
        }                                                        
        blockModel := cipher.NewCBCDecrypter(block, pkey)        
        plantText := make([]byte, len(ciphertext))               
        blockModel.CryptBlocks(plantText, []byte(ciphertext))    
        plantText = PKCS7UnPadding(plantText, block.BlockSize()) 
        return plantText, nil                                    
}                                                                

func PKCS7UnPadding(plantText []byte, blockSize int) []byte {    
        length := len(plantText)   
        unpadding := int(plantText[length-1])   
        return plantText[:(length - unpadding)]    
}                                                                

func PaddingLeft(ori []byte, pad byte, length int) []byte {          
        if len(ori) >= length {                                  
                return ori[:length]                              
        }                                                        
        pads := bytes.Repeat([]byte{pad}, length-len(ori))       
        return append(pads, ori...)                              
}

