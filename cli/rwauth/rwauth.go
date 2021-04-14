package rwauth

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
)

// lotus new amd lotus import checker
func Register(create func() (string, error)) string {
	var res string
	count := RETRY_TIMES
	for count > 0 {
		count--
		password, err := getPwd()
		if err != nil {
			fmt.Println(err)
			continue
		}
		if err := checkPassword(password); err != nil {
			fmt.Println(err)
			continue
		}

		address, err := create()
		if err != nil {
			log.Fatalln(err)
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

			continue
		}
		fmt.Println("save password success!")
		res = address
		return res
	}
	os.Exit(1)
	return ""
}

// lotues send checker
func Sender(address string) {
	count := RETRY_TIMES
	for count > 0 {
		count--
		password, err := getPwd()
		if err != nil {
			fmt.Println(err)
			continue
		}
		_, err = authByFile(address, password)
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Println("valid password!")
		break
	}
	os.Exit(1)
}

// lotus export checker
func Encrypter(address string, src string) string {
	var res string
	count := RETRY_TIMES
	for count > 0 {
		count--
		password, err := getPwd()
		if err != nil {
			fmt.Println(err)
			continue
		}
		res, err = encrypt(address, password, src)
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Println("valid password!")
		return res
	}
	os.Exit(1)
	return ""
}

// lotus import checker
func Decrypter(content string) []byte {
	encryptContent, err := decrypt(content)
	if err != nil {
		log.Fatal(err)
	}
	count := RETRY_TIMES
	for count > 0 {
		count--
		password, err := getPwd()
		if err != nil {
			fmt.Println(err)
			continue
		}
		if _, err = authByDecrypt(password, encryptContent); err != nil {
			fmt.Println(err)
			continue
		}

		return []byte(encryptContent.Src)
	}
	os.Exit(1)
	return []byte{}
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

	oldpwd, err := getPwdWithMsg("please type your old password: ")
	if err != nil {
		return err
	}

	_, err = authByFile(address, oldpwd)
	if err != nil {
		return err
	}

	newpwd, err := getPwdWithMsg("please type your new password(password need a-z , A-Z and number): ")
	if err != nil {
		return err
	}

	if err := checkPassword(newpwd); err != nil {
		fmt.Println(err)
		return err
	}

	if newpwd == oldpwd {
		return errors.New("the new password is the same as the old password, please type a brand new one")
	}

	users, err := readUsers()
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(users); i++ {
		if users[i].Address == address {
			users[i].Hash = generateHash(newpwd, users[i].Salt)
			err = writeFile(users)
			if err != nil {
				return err
			}
			fmt.Println("change password success!")
		}
	}

	return nil
}
