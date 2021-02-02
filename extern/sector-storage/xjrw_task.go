package sectorstorage

import (
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
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
var P2NumberLimit int

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

		if p1p2State != 0 {
			res := ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg32GiBV1_1]
			res.MaxParallelism = 1
			ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg32GiBV1_1] = res

			res = ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg2KiBV1_1]
			res.MaxParallelism = 1
			ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg2KiBV1_1] = res
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
	if p2Str := os.Getenv("P2_NUMBER"); p2Str != "" {
		if p2Num, err := strconv.Atoi(p2Str); err == nil {
			P2NumberLimit = p2Num
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
	if os.Getenv("DISPATCH_SERVER") == "" && p1p2State == 0 {
		panic("DISPATCH_SERVER not set")
	}
	http.ListenAndServe(os.Getenv("DISPATCH_SERVER"), nil)
}
func initServer(m *Manager) {
	http.HandleFunc("/getStatus", m.handlerStatus)
	miner := os.Getenv("MINER_SERVER")
	if miner == "" {
		panic("MINER_SERVER not set")
	}
	http.ListenAndServe(miner, nil)
}
