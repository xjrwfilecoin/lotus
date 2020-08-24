package sectorstorage

import (
	"encoding/json"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"io/ioutil"
	"os"
	"os/exec"
)

const sfil = "./taskconfig.json"

var taskState = map[string]map[sealtasks.TaskType]int{}

func init() {
	data, err := ioutil.ReadFile(sfil)
	if err != nil {
		//panic(err)
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
