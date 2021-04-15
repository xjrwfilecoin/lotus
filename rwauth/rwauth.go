package rwauth

import (
	"fmt"
	"log"
	"os/exec"
)

// lotus new amd lotus import checker
func Register(create func() (string, error)) (string, error) {
	password, err := getPwd()
	if err != nil {
		return "", err
	}
	if err := checkPassword(password); err != nil {
		return "", err
	}

	address, err := create()
	if err != nil {
		return "", err
	}

	err = registry(address, password)
	if err != nil {
		fmt.Println(err)
		//need to rollback
		cmd := exec.Command("lotus", "wallet", "delete", address)
		err := cmd.Run()
		if err != nil {
			log.Fatal(err)
		}

		return "", err
	}
	fmt.Println("save password success!")
	return address, nil
}

// lotues send checker
func Sender(address string) error {
	password, err := getPwd()
	if err != nil {
		return err
	}
	_, err = authByFile(address, password)
	if err != nil {
		return err
	}
	fmt.Println("valid password!")
	return nil
}

// lotus export checker
func Encrypter(address string, src string) (string, error) {
	var res string
	password, err := getPwd()
	if err != nil {
		return "", err
	}
	res, err = encrypt(address, password, src)
	if err != nil {
		return "", err
	}
	fmt.Println("valid password!")
	return res, nil
}

// lotus import checker
func Decrypter(content string) ([]byte, error) {
	encryptContent, err := decrypt(content)
	if err != nil {
		return nil, err
	}

	password, err := getPwd()
	if err != nil {
		return nil, err
	}
	if _, err = authByDecrypt(password, encryptContent); err != nil {
		return nil, err
	}

	return []byte(encryptContent.Src), nil

}

// lotus import checker
func IsEncrypted(content string) bool {
	if _, err := checkImportContent(content); err != nil {
		return false
	}
	return true
}

// delete user from the file while "lotus wallet delete" excute
func Delete(address string) {
	delete(address)
}

//lotus chpwd checker
func ChangePwd(address string) error {
	if err := checkAddress(address); err != nil {
		return err
	}

	if checkAddressIfExists(address) {
		return changePwd(address)
	} else {
		if _, err := Register(func() (string, error) { return address, nil }); err != nil {
			return err
		}
		return nil
	}
}

func EncryptDES_ECB(src string) (string, error) {
	return encryptDES_ECB(src)
}

func DecryptDES_ECB(src string) (string, error) {
	return decryptDES_ECB(src)
}
