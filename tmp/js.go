package main

import (
    "bytes"
    "crypto/aes"
    "fmt"
    "crypto/cipher"
)

func main(){
    st,_:=Decrypt([]byte("b7f1b2e55b663890ef85d357ee5ca5c6"),[]byte("12345678"))
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

