package rwauth

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"github.com/howeyc/gopass"
)

// checkPassword check pwd level
func checkPassword(ps string) error {
	if strings.TrimSpace(ps) == "" {
		return errors.New("password can not be null")
	}
	if len(ps) < 8 {
		return fmt.Errorf("password len is < 8")
	}
	num := `[0-9]{1}`
	a_z := `[a-z]{1}`
	A_Z := `[A-Z]{1}`
	// symbol := `[!@#~$%^&*()+|_]{1}`
	if b, err := regexp.MatchString(num, ps); !b || err != nil {
		return fmt.Errorf("password need number :%v", err)
	}
	if b, err := regexp.MatchString(a_z, ps); !b || err != nil {
		return fmt.Errorf("password need a-z :%v", err)
	}
	if b, err := regexp.MatchString(A_Z, ps); !b || err != nil {
		return fmt.Errorf("password need A-Z :%v", err)
	}
	// if b, err := regexp.MatchString(symbol, ps); !b || err != nil {
	// 	return fmt.Errorf("password need symbol :%v", err)
	// }
	return nil
}

func checkAddress(address string) error {
	if strings.TrimSpace(address) == "" {
		return errors.New("address can not be null")
	}
	return nil
}

// readUsers read users from file
func readUsers() ([]User, error) {
	if _, err := os.Stat(filePath); err != nil {
		file, err2 := os.Create(filePath)
		if err2 != nil {
			return nil, err2
		}
		defer file.Close()
		file.Write([]byte(encoding([]byte("[]"))))
	}

	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	bytes, err = decoding(string(bytes))

	if err != nil {
		return nil, err
	}

	var users []User

	err = json.Unmarshal(bytes, &users)

	if err != nil {
		return nil, err
	}
	return users, nil
}

// writeFile write user to the file
func writeFile(users []User) error {
	bytes, err := json.Marshal(users)
	if err != nil {
		return err
	}

	os.WriteFile(filePath, []byte(encoding(bytes)), 0766)
	return nil
}

// check address is exists
func checkAddressIfExists(address string) bool {
	users, err := readUsers()
	if err != nil {
		return false
	}
	for i := 0; i < len(users); i++ {
		if address == users[i].Address {
			return true
		}
	}
	return false
}

// registry a user
func registry(address string, password string) error {
	users, err := readUsers()
	if err != nil {
		return err
	}
	for i := 0; i < len(users); i++ {
		if address == users[i].Address {
			return errors.New("address already exists")
		}
	}

	if err := checkAddress(address); err != nil {
		return err
	}

	if err := checkPassword(password); err != nil {
		return err
	}

	salt := uuid.New().String()

	user := User{
		Address: address,
		Salt:    salt,
		Hash:    generateHash(password, salt),
	}

	if err != nil {
		return err
	}
	users = append(users, user)

	return writeFile(users)
}

// auth dalidate the password is correct or not
func auth(address string, password string, users []User) (*User, error) {
	if err := checkAddress(address); err != nil {
		return nil, err
	}
	if err := checkPassword(password); err != nil {
		return nil, err
	}

	var user *User
	for _, item := range users {
		if item.Address == address {
			user = &item
			break
		}
	}
	if user == nil {
		return nil, errors.New("the address does not exist")
	}
	if generateHash(password, user.Salt) != user.Hash {
		return nil, errors.New("invalid password")
	}
	return user, nil
}

// auth by the users from the fle
func authByFile(address string, password string) (*User, error) {
	users, err := readUsers()
	if err != nil {
		return nil, err
	}
	return auth(address, password, users)
}

// auth by the users from the encrypt content
func authByDecrypt(password string, content EncryptContent) (*User, error) {
	users := []User{content.User}
	address := content.User.Address

	res, err := auth(address, password, users)

	//add to file
	if err == nil {
		data, err := readUsers()
		if err != nil {
			return nil, err
		}
		for i := 0; i < len(data); i++ {
			if address == data[i].Address {
				return nil, errors.New("address already exists")
			}
		}

		if err := checkPassword(password); err != nil {
			return nil, err
		}

		data = append(data, users...)
		if err = writeFile(data); err != nil {
			return nil, err
		}
	}
	return res, err
}

// encrypt the content to a secret
func encrypt(address string, password string, src string) (string, error) {
	user, err := authByFile(address, password)
	if err != nil {
		return "", err
	}

	return encryptDES_ECB(fmt.Sprintf("~~%s~~%s~~%s~~%s", user.Address, user.Hash, user.Salt, src))
}

// decrypt the secret to content
func decrypt(src string) (EncryptContent, error) {
	whole, err := decryptDES_ECB(src)
	if err != nil {
		return EncryptContent{}, err
	}

	reg := regexp.MustCompile(`^~~(.+)~~(.+)~~(.+)~~(.+)`)
	slice := reg.FindStringSubmatch(whole)

	res := EncryptContent{User: User{Address: slice[1], Hash: slice[2], Salt: slice[3]}, Src: slice[4]}

	return res, nil
}

// get password with msg
func getPwdWithMsg(msg string) (string, error) {
	res, err := gopass.GetPasswdPrompt(msg, true, os.Stdout, os.Stdin)
	return string(res), err
}

// get passord by default msg "please type your password(password need a-z , A-Z and number): "
func getPwd() (string, error) {
	return getPwdWithMsg("please type your password(password need a-z , A-Z and number): ")
}

// delete user from the file
func delete(address string) error {
	if err := checkAddress(address); err != nil {
		return err
	}
	users, err := readUsers()
	if err != nil {
		log.Fatal(err)
	}
	length := len(users)
	var res []User
	for i := length - 1; i >= 0; i-- {
		if users[i].Address == address {
			res = append(res, users[:i]...)
			res = append(res, users[i+1:]...)
			return writeFile(res)
		}
	}
	return errors.New(fmt.Sprintf("can not find this address: %s", address))
}

// change pwd
func changePwd(address string) error {
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
