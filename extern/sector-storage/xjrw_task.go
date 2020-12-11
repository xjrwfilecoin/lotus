package sectorstorage

import (
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
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
var p2SpaceLimit int64
var p1Limit int
var p2Limit int
var c2Limit int
var p1p2State int

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

func InitTask() {
	if str := os.Getenv("P2_SPACE"); str != "" {
		if p2SpaceNum, err := strconv.ParseInt(str, 10, 64); err == nil {
			p2SpaceLimit = p2SpaceNum
		}
	}

	if str := os.Getenv("P1P2_STATE"); str != "" {
		if p1p2, err := strconv.Atoi(str); err == nil {
			p1p2State = p1p2
			fmt.Println("P1P2_STATE", p1p2State)
		}
	}

	if str := os.Getenv("P1_LIMIT"); str != "" {
		if p1Num, err := strconv.Atoi(str); err == nil {
			p1Limit = p1Num
			fmt.Println("P1_LIMIT", p1Limit)
		}
	}

	if str := os.Getenv("P2_LIMIT"); str != "" {
		if p2Num, err := strconv.Atoi(str); err == nil {
			p2Limit = p2Num
			fmt.Println("P2_LIMIT", p2Limit)
		}
	}

	if str := os.Getenv("C2_LIMIT"); str != "" {
		if c2Num, err := strconv.Atoi(str); err == nil {
			c2Limit = c2Num
			fmt.Println("C2_LIMIT", c2Limit)
		}
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
