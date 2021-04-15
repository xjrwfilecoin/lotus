package rwauth

import (
	"bytes"
	"crypto/des"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
)

const (
	PRIVATEKEY  = "89357601"
	PREFIX      = "encrypt:"
	RETRY_TIMES = 3
	FILE_NAME   = ".rwauth"
)

var filePath string

func init() {
	lsp := os.Getenv("LOTUS_PATH")
	if lsp != "" {
		_, err := os.Stat(lsp)
		if err == nil {
			filePath = path.Join(lsp, FILE_NAME)
			return
		}
	}
	filePath = path.Join("./", FILE_NAME)
}

type User struct {
	Address string
	Hash    string
	Salt    string
}

type EncryptContent struct {
	User User
	Src  string
}

// checkImportContent content is encrypted or not
func checkImportContent(content string) (string, error) {
	if len(content) < len(PREFIX) {
		return "", errors.New("illegal content")
	}
	if content[:len(PREFIX)] != PREFIX {
		return "", errors.New("illegal content")
	}
	return content[len(PREFIX):], nil
}

// encryptDES_ECB ecb encrypt
func encryptDES_ECB(src string) (string, error) {
	data := []byte(src)
	keyByte := []byte(PRIVATEKEY)
	block, err := des.NewCipher(keyByte)
	if err != nil {
		panic(err)
	}
	bs := block.BlockSize()
	data = pKCS5Padding(data, bs)
	if len(data)%bs != 0 {
		return "", errors.New("need a multiple of the blocksize")
	}
	out := make([]byte, len(data))
	dst := out
	for len(data) > 0 {
		block.Encrypt(dst, data[:bs])
		data = data[bs:]
		dst = dst[bs:]
	}
	return fmt.Sprintf("%s%X", PREFIX, out), nil
}

// decryptDES_ECB ecb decrypt
func decryptDES_ECB(src string) (string, error) {
	var err error
	if src, err = checkImportContent(src); err != nil {
		return "", err
	}
	data, err := hex.DecodeString(src)
	if err != nil {
		panic(err)
	}
	keyByte := []byte(PRIVATEKEY)
	block, err := des.NewCipher(keyByte)
	if err != nil {
		panic(err)
	}
	bs := block.BlockSize()
	if len(data)%bs != 0 {
		return "", errors.New("rypto/cipher: input not full blocks")
	}
	out := make([]byte, len(data))
	dst := out
	for len(data) > 0 {
		block.Decrypt(dst, data[:bs])
		data = data[bs:]
		dst = dst[bs:]
	}
	out = pKCS5UnPadding(out)
	return string(out), nil
}

// encoding base64
func encoding(bytes []byte) string {
	return base64.StdEncoding.EncodeToString(bytes)
}

// decoding base64
func decoding(encoded string) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

// pKCS5Padding padding
func pKCS5Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

// pKCS5UnPadding unpadding
func pKCS5UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

// generateHash generate a hash from password and salt
func generateHash(password string, salt string) string {
	h := sha256.New()
	h.Write([]byte(password + salt))
	return hex.EncodeToString(h.Sum(nil))
}
