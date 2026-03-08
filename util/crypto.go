package util

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"errors"
	"fmt"
)

func ByteToHexStr(b []byte) string {
	//字节数组转16进制字符串
	return hex.EncodeToString(b)
}

func HexStrToByte(s string) ([]byte, error) {
	//16进制字符串转字节数组erros
	d, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func PKCS7Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func PKCS7UnPadding(origData []byte) []byte { // 去码
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

func AESEncrypt(orig string, key string) string {
	// 转成字节数组
	origData := []byte(orig)
	k := []byte(key)
	// 分组秘钥
	// NewCipher该函数限制了输入k的长度必须为16, 24或者32
	block, _ := aes.NewCipher(k)
	// 获取秘钥块的长度
	blockSize := block.BlockSize()
	// 补全码
	origData = PKCS7Padding(origData, blockSize)
	// 加密模式
	blockMode := cipher.NewCBCEncrypter(block, k[:blockSize])
	// 创建数组
	crypted := make([]byte, len(origData))
	// 加密
	blockMode.CryptBlocks(crypted, origData)
	//return base64.StdEncoding.EncodeToString(crypted)
	return ByteToHexStr(crypted)
}

func AESDecrypt(crypted string, key string) (str string, err error) {
	// 解密
	var cryptedByte []byte
	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprintf("%s", r))
		}
	}()

	// 转成字节数组
	cryptedByte, err = HexStrToByte(crypted)
	if err != nil {
		return "", err
	}
	k := []byte(key)
	// 分组秘钥
	block, _ := aes.NewCipher(k)
	// 获取秘钥块的长度
	blockSize := block.BlockSize()
	// 加密模式
	blockMode := cipher.NewCBCDecrypter(block, k[:blockSize])
	// 创建数组
	orig := make([]byte, len(cryptedByte))
	// 解密
	blockMode.CryptBlocks(orig, cryptedByte)
	// 去补全码
	orig = PKCS7UnPadding(orig)
	str = string(orig)
	return str, nil
}
