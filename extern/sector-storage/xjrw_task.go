package sectorstorage

import (
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"io/ioutil"
	"net/http"
	"os"
)

const sfiltask = "./taskconfig.json"
const sfilgroup = "./groupconfig.json"

type GroupConfig struct {
	GroupName  string
	GroupIndex int
}

var taskState = map[string]map[sealtasks.TaskType]int{}
var groupState = map[string]GroupConfig{}
var groupCount = map[string]int{}

func loadGroup() {
	data, err := ioutil.ReadFile(sfilgroup)
	if err != nil {
		panic(err)
		return
	}
	err = json.Unmarshal(data, &groupState)
	if err != nil {
		panic(err)
	}
	fmt.Printf("group : %v \n", groupState)
}

func loadTask() {
	data, err := ioutil.ReadFile(sfiltask)
	if err != nil {
		//panic(err)
		return
	}
	err = json.Unmarshal(data, &taskState)
	if err != nil {
		panic(err)
	}
}

func getGroupCount(groupName string) int {
	sum := 0

	if sum, ok := groupCount[groupName]; ok {
		return sum
	}

	for _, group := range groupState {
		if group.GroupName == groupName && group.GroupIndex != -1 {
			sum++
		}
	}
	groupCount[groupName] = sum
	return sum
}

func initDispatchServer(m *Manager) {
	http.HandleFunc("/getHost", m.handlerP2)
	http.HandleFunc("/setFinish", m.handlerP1)
	if os.Getenv("DISPATCH_SERVER") == "" {
		panic("DISPATCH_SERVER not set")
	}
	http.ListenAndServe(os.Getenv("DISPATCH_SERVER"), nil)
}
