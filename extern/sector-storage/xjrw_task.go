package sectorstorage

import (
	"encoding/json"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
)

const sfil = "./taskconfig.json"

type TaskState struct {
	TaskType sealtasks.TaskType
	TaskNum  int
}

var taskState = map[string]*TaskState{}

func init() {
	data, err := ioutil.ReadFile(sfil)
	if err != nil {
		panic(err)
		return
	}
	err = json.Unmarshal(data, &taskState)
	if err != nil {
		panic(err)
	}

}

func ShellExecute(cmdStr string) error {
	cmd := exec.Command("/bin/bash", "-c", cmdStr, "|sh")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	err := cmd.Run()
	log.Infof("ShellExecute %s : %v", cmdStr, err)
	return err
}

func RemoveFile(fileName string) error {
	file := filepath.Join(os.Getenv("TMPDIR"), fileName)
	cmd := "rm -f " + file
	return ShellExecute(cmd)
}
