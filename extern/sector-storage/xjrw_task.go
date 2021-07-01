package sectorstorage

import (
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
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
var p1SpaceLimit int
var p2SpaceLimit int64
var p1Limit int
var p2Limit int
var c2Limit int
var p1p2State int
var P2NumberLimit int
var autoInterval int
var WindowsPostLk bool
var finalizeLk sync.Mutex

func InitTask(b bool) {
	autoInterval = 5
	initConf(b)
	if str := os.Getenv("P1P2_STATE"); str != "" {
		if p1p2, err := strconv.Atoi(str); err == nil {
			p1p2State = p1p2
		}

		if p1p2State != 0 {
			res := ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg32GiBV1_1]
			res.MaxParallelism = 0
			ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg32GiBV1_1] = res

			res = ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg64GiBV1_1]
			res.MaxParallelism = 0
			ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg64GiBV1_1] = res

			res = ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg2KiBV1_1]
			res.MaxParallelism = 0
			ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg2KiBV1_1] = res
		}
	}

	if str := os.Getenv("P1_SPACE"); str != "" {
		if data, err := strconv.Atoi(str); err == nil {
			p1SpaceLimit = data
		}
	}

	ffiwrapper.InitData(b)
	stores.InitData()

	fmt.Printf("P1_SPACE = %v, P2_SPACE = %v, AUTO_INTERVAL_TIME = %v, P1P2_STATE = %v, P1_LIMIT = %v, P2_LIMIT = %v, C2_LIMIT = %v, P2_NUMBER = %v \n", p1SpaceLimit, p2SpaceLimit, autoInterval, p1p2State, p1Limit, p2Limit, c2Limit, P2NumberLimit)
}

func initConf(b bool) bool {
	data, err := ioutil.ReadFile("./config.json")
	if err != nil {
		e := fmt.Sprintf("err: %v ", err)
		fmt.Println(e)
		if b {
			panic(e)
		}
		return false
	}

	var conf = map[string]int{}
	err = json.Unmarshal(data, &conf)
	if err != nil {
		e := fmt.Sprintf("config.json: %v ", err)
		fmt.Println(e)
		if b {
			panic(e)
		}
		return false
	}

	if conf["AUTO_INTERVAL_TIME"] < 0 {
		e := "AUTO_INTERVAL_TIME must be greater than 0"
		fmt.Println(e)
		if b {
			panic(e)
		}
		return false
	}

	if conf["P2_SPACE"] < 0 {
		e := "P2_SPACE must be greater than 0"
		fmt.Println(e)
		if b {
			panic(e)
		}
		return false
	}

	if conf["P1_LIMIT"] < 0 {
		e := "P1_LIMIT must be greater than 0"
		fmt.Println(e)
		if b {
			panic(e)
		}
		return false
	}

	if conf["P2_LIMIT"] < 0 {
		e := "P2_LIMIT must be greater than 0"
		fmt.Println(e)
		if b {
			panic(e)
		}
		return false
	}

	if conf["C2_LIMIT"] < 0 {
		e := "C2_LIMIT must be greater than 0"
		fmt.Println(e)
		if b {
			panic(e)
		}
		return false
	}

	if conf["P2_NUMBER"] < 0 {
		e := "P2_NUMBER must be greater than 0"
		fmt.Println(e)
		if b {
			panic(e)
		}
		return false
	}

	autoInterval = conf["AUTO_INTERVAL_TIME"]

	if conf["P2_SPACE"] > 0 {
		p2SpaceLimit = int64(conf["P2_SPACE"])
	}

	if conf["P1_LIMIT"] > 0 {
		p1Limit = conf["P1_LIMIT"]
	}

	if conf["P2_LIMIT"] > 0 {
		p2Limit = conf["P2_LIMIT"]
	}

	if conf["C2_LIMIT"] > 0 {
		c2Limit = conf["C2_LIMIT"]
	}

	if conf["P2_NUMBER"] > 0 {
		P2NumberLimit = conf["P2_NUMBER"]
	}

	if p2Limit > 0 {
		res := ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg32GiBV1_1]
		res.MaxParallelism = 1
		ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg32GiBV1_1] = res

		res = ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg64GiBV1_1]
		res.MaxParallelism = 1
		ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg64GiBV1_1] = res

		res = ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg2KiBV1_1]
		res.MaxParallelism = 1
		ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg2KiBV1_1] = res
	}

	if c2Limit > 0 {
		res := ResourceTable[sealtasks.TTCommit2][abi.RegisteredSealProof_StackedDrg32GiBV1_1]
		res.MaxParallelism = 1
		ResourceTable[sealtasks.TTCommit2][abi.RegisteredSealProof_StackedDrg32GiBV1_1] = res

		res = ResourceTable[sealtasks.TTCommit2][abi.RegisteredSealProof_StackedDrg64GiBV1_1]
		res.MaxParallelism = 1
		ResourceTable[sealtasks.TTCommit2][abi.RegisteredSealProof_StackedDrg64GiBV1_1] = res

		res = ResourceTable[sealtasks.TTCommit2][abi.RegisteredSealProof_StackedDrg2KiBV1_1]
		res.MaxParallelism = 1
		ResourceTable[sealtasks.TTCommit2][abi.RegisteredSealProof_StackedDrg2KiBV1_1] = res
	}

	return true
}

func initDispatchServer(m *Manager) {
	if p1p2State != 0 {
		return
	}
	http.HandleFunc("/getHost", m.handlerP2)
	//http.HandleFunc("/setFinish", m.handlerP1)
	dispatch := os.Getenv("DISPATCH_SERVER")
	if dispatch == "" {
		panic("DISPATCH_SERVER not set")
	}
	http.ListenAndServe(dispatch, nil)
}

func initServer(m *Manager) {
	http.HandleFunc("/getStatus", m.handlerStatus)
	miner := os.Getenv("MINER_SERVER")
	if miner == "" {
		panic("MINER_SERVER not set")
	}
	http.ListenAndServe(miner, nil)
}
