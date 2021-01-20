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
var hostMap = map[string]WorkerID{}
var autoInterval int

func InitTask() {
	autoInterval = 5
	initConf(true)
	if str := os.Getenv("P1P2_STATE"); str != "" {
		if p1p2, err := strconv.Atoi(str); err == nil {
			p1p2State = p1p2
		}

		if p1p2State != 0 {
			res := ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg32GiBV1_1]
			res.MaxParallelism = 0
			ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg32GiBV1_1] = res

			res = ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg2KiBV1_1]
			res.MaxParallelism = 0
			ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg2KiBV1_1] = res
		}
	}


	fmt.Printf("P2_SPACE = %v, AUTO_INTERVAL_TIME = %v, P1P2_STATE = %v, P1_LIMIT = %v, P2_LIMIT = %v, C2_LIMIT = %v, P2_NUMBER = %v \n", p2SpaceLimit, autoInterval, p1p2State, p1Limit, p2Limit, c2Limit, P2NumberLimit)
}

func initConf(b bool) {
	data, err := ioutil.ReadFile("./config.json")
	if err != nil {
		fmt.Println("err: ", err)
		if b {
			panic(fmt.Sprintf("err: %v ", err))
		}
		return
	}

	var conf = map[string]int{}
	err = json.Unmarshal(data, &conf)
	if err != nil {
		fmt.Println("config.json: ", err)
		if b {
			panic(fmt.Sprintf("config.json: %v ", err))
		}
		return
	}

	autoInterval = conf["AUTO_INTERVAL_TIME"]

	p2SpaceLimit = int64(conf["P2_SPACE"])
	p1Limit = conf["P1_LIMIT"]
	p2Limit = conf["P2_LIMIT"]
	c2Limit = conf["C2_LIMIT"]
	P2NumberLimit = conf["P2_NUMBER"]
	if p2Limit > 0 {
		res := ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg32GiBV1_1]
		res.MaxParallelism = 1
		ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg32GiBV1_1] = res

		res = ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg2KiBV1_1]
		res.MaxParallelism = 1
		ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg2KiBV1_1] = res
	}

	if c2Limit > 0 {
		res := ResourceTable[sealtasks.TTCommit2][abi.RegisteredSealProof_StackedDrg32GiBV1_1]
		res.MaxParallelism = 1
		ResourceTable[sealtasks.TTCommit2][abi.RegisteredSealProof_StackedDrg32GiBV1_1] = res

		res = ResourceTable[sealtasks.TTCommit2][abi.RegisteredSealProof_StackedDrg2KiBV1_1]
		res.MaxParallelism = 1
		ResourceTable[sealtasks.TTCommit2][abi.RegisteredSealProof_StackedDrg2KiBV1_1] = res
	}
}

func initDispatchServer(m *Manager) {
	if p1p2State != 0 {
		return
	}
	http.HandleFunc("/getHost", m.handlerP2)
	http.HandleFunc("/setFinish", m.handlerP1)
	dispatch := os.Getenv("DISPATCH_SERVER")
	if dispatch == "" {
		panic("DISPATCH_SERVER not set")
	}
	http.ListenAndServe(dispatch, nil)
}
