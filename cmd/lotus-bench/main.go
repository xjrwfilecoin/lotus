package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/docker/go-units"
	sectorstorage "github.com/filecoin-project/lotus/extern/sector-storage"
	logging "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	paramfetch "github.com/filecoin-project/go-paramfetch"
	"github.com/filecoin-project/go-state-types/abi"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper/basicfs"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/types"
	sealing "github.com/filecoin-project/lotus/extern/storage-sealing"
)

var log = logging.Logger("lotus-bench")

type BenchResults struct {
	EnvVar map[string]string

	SectorSize   abi.SectorSize
	SectorNumber int

	SealingSum     SealingResult
	SealingResults []SealingResult

	PostGenerateCandidates time.Duration
	PostWinningProofCold   time.Duration
	PostWinningProofHot    time.Duration
	VerifyWinningPostCold  time.Duration
	VerifyWinningPostHot   time.Duration

	PostWindowProofCold  time.Duration
	PostWindowProofHot   time.Duration
	VerifyWindowPostCold time.Duration
	VerifyWindowPostHot  time.Duration
}

func (bo *BenchResults) SumSealingTime() error {
	if len(bo.SealingResults) <= 0 {
		return xerrors.Errorf("BenchResults SealingResults len <= 0")
	}
	if len(bo.SealingResults) != bo.SectorNumber {
		return xerrors.Errorf("BenchResults SealingResults len(%d) != bo.SectorNumber(%d)", len(bo.SealingResults), bo.SectorNumber)
	}

	for _, sealing := range bo.SealingResults {
		bo.SealingSum.AddPiece += sealing.AddPiece
		bo.SealingSum.PreCommit1 += sealing.PreCommit1
		bo.SealingSum.PreCommit2 += sealing.PreCommit2
		bo.SealingSum.Commit1 += sealing.Commit1
		bo.SealingSum.Commit2 += sealing.Commit2
		bo.SealingSum.Verify += sealing.Verify
		bo.SealingSum.Unseal += sealing.Unseal
	}
	return nil
}

type SealingResult struct {
	AddPiece   time.Duration
	PreCommit1 time.Duration
	PreCommit2 time.Duration
	Commit1    time.Duration
	Commit2    time.Duration
	Verify     time.Duration
	Unseal     time.Duration
}

type Commit2In struct {
	SectorNum  int64
	Phase1Out  []byte
	SectorSize uint64
}

var tasks sync.Map

func main() {
	logging.SetLogLevel("*", "INFO")

	log.Info("Starting lotus-bench")

	app := &cli.App{
		Name:    "lotus-bench",
		Usage:   "Benchmark performance of lotus on your hardware",
		Version: build.UserVersion(),
		Commands: []*cli.Command{
			proveCmd,
			sealBenchCmd,
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Warnf("%+v", err)
		return
	}
}

var sealBenchCmd = &cli.Command{
	Name:  "sealing",
	Usage: "Benchmark seal and winning post and window post",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "storage-dir",
			Value: "~/.lotus-bench",
			Usage: "path to the storage directory that will store sectors long term",
		},
		&cli.StringFlag{
			Name:  "sector-size",
			Value: "512MiB",
			Usage: "size of the sectors in bytes, i.e. 32GiB",
		},
		&cli.BoolFlag{
			Name:  "no-gpu",
			Usage: "disable gpu usage for the benchmark run",
		},
		&cli.StringFlag{
			Name:  "miner-addr",
			Usage: "pass miner address (only necessary if using existing sectorbuilder)",
			Value: "t01000",
		},
		&cli.StringFlag{
			Name:  "benchmark-existing-sectorbuilder",
			Usage: "pass a directory to run post timings on an existing sectorbuilder",
		},
		&cli.BoolFlag{
			Name:  "json-out",
			Usage: "output results in json format",
		},
		&cli.BoolFlag{
			Name:  "skip-commit2",
			Usage: "skip the commit2 (snark) portion of the benchmark",
		},
		&cli.BoolFlag{
			Name:  "skip-unseal",
			Usage: "skip the unseal portion of the benchmark",
		},
		&cli.StringFlag{
			Name:  "ticket-preimage",
			Usage: "ticket random",
		},
		&cli.StringFlag{
			Name:  "save-commit2-input",
			Usage: "save commit2 input to a file",
		},
		&cli.IntFlag{
			Name:  "num-sectors",
			Usage: "select number of sectors to seal",
			Value: 4,
		},
		&cli.IntFlag{
			Name:  "parallel",
			Usage: "num run in parallel",
			Value: 1,
		},
	},
	Action: func(c *cli.Context) error {
		if c.Bool("no-gpu") {
			err := os.Setenv("BELLMAN_NO_GPU", "1")
			if err != nil {
				return xerrors.Errorf("setting no-gpu flag: %w", err)
			}
		}

		robench := c.String("benchmark-existing-sectorbuilder")

		var sbdir string

		if robench == "" {
			sdir, err := homedir.Expand(c.String("storage-dir"))
			if err != nil {
				return err
			}

			err = os.MkdirAll(sdir, 0775) //nolint:gosec
			if err != nil {
				return xerrors.Errorf("creating sectorbuilder dir: %w", err)
			}

			tsdir := sdir

			// TODO: pretty sure this isnt even needed?
			if err := os.MkdirAll(tsdir, 0775); err != nil {
				return err
			}

			sbdir = tsdir
		} else {
			exp, err := homedir.Expand(robench)
			if err != nil {
				return err
			}
			sbdir = exp
		}
		// sector size
		sectorSizeInt, err := units.RAMInBytes(c.String("sector-size"))
		if err != nil {
			return err
		}
		sectorSize := abi.SectorSize(sectorSizeInt)

		// Only fetch parameters if actually needed
		skipc2 := c.Bool("skip-commit2")
		if !skipc2 {
			if err := paramfetch.GetParams(lcli.ReqContext(c), build.ParametersJSON(), uint64(sectorSize)); err != nil {
				return xerrors.Errorf("getting params: %w", err)
			}
		}

		sbfs := &basicfs.Provider{
			Root: sbdir,
		}

		sb, err := ffiwrapper.New(sbfs)
		if err != nil {
			return err
		}

		sectorNumber := c.Int("num-sectors")

		sectorstorage.ShellExecute("rm -rf " + filepath.Join(os.Getenv("FIL_PROOFS_SSD_PARENT"), "*"))

		os.Mkdir(filepath.Join(os.Getenv("WORKER_PATH"), "undo"), 0755)
		os.Mkdir(filepath.Join(os.Getenv("WORKER_PATH"), "faults"), 0755)

		sectorstorage.ShellExecute("mv " + filepath.Join(filepath.Join(os.Getenv("WORKER_PATH"), "undo"), "back.json") + " " + filepath.Join(os.Getenv("WORKER_PATH"), "faults"))

		if robench == "" {
			err := runSeals(sb, sectorSize, sectorNumber)
			if err != nil {
				return xerrors.Errorf("failed to run seals: %w", err)
			}
		}

		stop := make(chan struct{})
		<-stop

		return nil
	},
}

type ParCfg struct {
	PreCommit1 int
	PreCommit2 int
	Commit     int
}

type SectorInfo struct {
	Miner  string
	Random string
}

type P1Info struct {
	Miner string
	out   storage.PreCommit1Out
}

func scanDir(dirName string) []string {
	files, err := ioutil.ReadDir(dirName)
	if err != nil {
		log.Info(err)
		return nil
	}
	var fileList []string
	for _, file := range files {
		path := dirName + string(os.PathSeparator) + file.Name()
		fileList = append(fileList, path)
	}
	return fileList
}

func ReadJson(fileName string) (map[string]SectorInfo, error) {
	state := make(map[string]SectorInfo)
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Info("read json err: ", err)
		return map[string]SectorInfo{}, err
	}

	err = json.Unmarshal(data, &state)
	if err != nil {
		log.Info("Unmarshal json err : ", err)
		return map[string]SectorInfo{}, err
	}
	return state, nil
}

func WriteJson(id int) error {
	if id != -1 {
		tasks.Delete(id)
	}

	state := make(map[string]SectorInfo)

	tasks.Range(func(k, v interface{}) bool {
		state[strconv.Itoa(k.(int))] = v.(SectorInfo)
		return true
	})

	file := filepath.Join(filepath.Join(os.Getenv("WORKER_PATH"), "undo"), "back.json")
	os.Remove(file)
	f, err := os.Create(file)
	if err != nil {
		fmt.Println("err :", err)
		return err
	}
	defer f.Close()

	d, err := json.MarshalIndent(state, "", " ")
	if err != nil {
		fmt.Println("err :", err)
		return err
	}

	f.Write(d)

	return nil
}

func deletefiles(id abi.SectorID) {
	cachePath := filepath.Join(os.Getenv("WORKER_PATH"), "cache")
	destPath := filepath.Join(cachePath, storiface.SectorName(id))
	sectorstorage.ShellExecute("rm -rf " + destPath)

	sealedPath := filepath.Join(os.Getenv("WORKER_PATH"), "sealed")
	destPath = filepath.Join(sealedPath, storiface.SectorName(id))
	sectorstorage.ShellExecute("rm -rf " + destPath)

	unsealedPath := filepath.Join(os.Getenv("WORKER_PATH"), "unsealed")
	destPath = filepath.Join(unsealedPath, storiface.SectorName(id))
	sectorstorage.ShellExecute("rm -rf " + destPath)
}

func revertID(addr string) abi.ActorID {
	maddr, err := address.NewFromString(addr)
	if err != nil {
		return 0
	}
	amid, err := address.IDFromAddress(maddr)
	if err != nil {
		return 0
	}
	return abi.ActorID(amid)
}

func runSeals(sb *ffiwrapper.Sealer, sectorSize abi.SectorSize, sectorNumber int) error {
	var rP1 sync.Map
	var rP2 sync.Map
	ids := make(map[int]SectorInfo)

	go func() {
		for {
			filesPath := scanDir(filepath.Join(os.Getenv("WORKER_PATH"), "faults"))
			for _, path := range filesPath {
				log.Info("read file ", path)
				if state, err := ReadJson(path); err == nil {
					for id, info := range state {
						if id, err := strconv.Atoi(id); err == nil {
							deletefiles(abi.SectorID{
								Miner:  revertID(info.Miner),
								Number: abi.SectorNumber(id),
							})
							ids[id] = info
							tasks.Store(id, info)
						}
					}
					os.Remove(path)
					log.Info("remove file ", path)
				} else {
					log.Info("file err: ", err)
				}
			}

			if len(filesPath) > 0 {
				WriteJson(-1)
			}

			log.Info("task: ", ids)

			for id, info := range ids {
				length := 0
				rP1.Range(func(k, v interface{}) bool {
					length++
					return true
				})

				if length < sectorNumber {
					delete(ids, id)
					rP1.Store(id, info)
					go func(id int, info SectorInfo) error {
						defer rP1.Delete(id)
						sid := storage.SectorRef{
							ID: abi.SectorID{
								Miner:  revertID(info.Miner),
								Number: abi.SectorNumber(id),
							},
							ProofType: spt(sectorSize),
						}
						log.Info("p1 start ", sid)

						size := abi.PaddedPieceSize(sectorSize).Unpadded()
						pi, err := sb.AddPiece(context.TODO(), sid, nil, size, sealing.NewNullReader(size))
						if err != nil {
							log.Infof("p1 AddPiece failed: %v %v", sid, err)
							return err
						}

						sDec, err := base64.StdEncoding.DecodeString(info.Random)
						if err != nil {
							log.Infof("p1 DecodeString failed: %v %v", sid, err)
							return err
						}

						p1out, err := sb.SealPreCommit1(context.TODO(), sid, sDec, []abi.PieceInfo{pi})
						if err != nil {
							log.Infof("p1 failed %v : %v", sid, err)
							return xerrors.Errorf("commit: %w", err)
						}

						rP2.Store(id, P1Info{
							Miner: info.Miner,
							out:   p1out,
						})

						log.Infof("p1 finish: %v", sid)

						return nil
					}(id, info)
				}
			}
			<-time.After(time.Second * 1)
		}
	}()

	go func() {
		for {
			id := -1
			var p1out storage.PreCommit1Out
			var miner string
			rP2.Range(func(k, v interface{}) bool {
				id = k.(int)
				miner = (v.(P1Info)).Miner
				p1out = (v.(P1Info)).out
				return true
			})

			if id != -1 {
				rP2.Delete(id)
				sid := storage.SectorRef{
					ID: abi.SectorID{
						Miner:  revertID(miner),
						Number: abi.SectorNumber(id),
					},
					ProofType: spt(sectorSize),
				}

				log.Info("p2 start ", sid)
				_, err := sb.SealPreCommit2(context.TODO(), sid, p1out)
				if err != nil {
					log.Infof("p2 failed %v : %v", sid, err)
					continue
				}

				WriteJson(id)

				log.Info("p2 finish ", sid)
				cachePath := filepath.Join(os.Getenv("WORKER_PATH"), "cache")
				destPath := filepath.Join(cachePath, storiface.SectorName(sid.ID))
				sectorstorage.ShellExecute("rm -rf " + filepath.Join(destPath, "sc-02-data-tree-c*"))
				sectorstorage.ShellExecute("rm -rf " + filepath.Join(destPath, "sc-02-data-tree-d*"))
				sectorstorage.ShellExecute("rm -rf " + filepath.Join(destPath, "sc-02-data-layer*"))
			}

			<-time.After(time.Second * 1)
		}
	}()

	return nil
}

var proveCmd = &cli.Command{
	Name:      "prove",
	Usage:     "Benchmark a proof computation",
	ArgsUsage: "[input.json]",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "no-gpu",
			Usage: "disable gpu usage for the benchmark run",
		},
		&cli.StringFlag{
			Name:  "miner-addr",
			Usage: "pass miner address (only necessary if using existing sectorbuilder)",
			Value: "t01000",
		},
	},
	Action: func(c *cli.Context) error {
		if c.Bool("no-gpu") {
			err := os.Setenv("BELLMAN_NO_GPU", "1")
			if err != nil {
				return xerrors.Errorf("setting no-gpu flag: %w", err)
			}
		}

		if !c.Args().Present() {
			return xerrors.Errorf("Usage: lotus-bench prove [input.json]")
		}

		inb, err := ioutil.ReadFile(c.Args().First())
		if err != nil {
			return xerrors.Errorf("reading input file: %w", err)
		}

		var c2in Commit2In
		if err := json.Unmarshal(inb, &c2in); err != nil {
			return xerrors.Errorf("unmarshalling input file: %w", err)
		}

		if err := paramfetch.GetParams(lcli.ReqContext(c), build.ParametersJSON(), c2in.SectorSize); err != nil {
			return xerrors.Errorf("getting params: %w", err)
		}

		maddr, err := address.NewFromString(c.String("miner-addr"))
		if err != nil {
			return err
		}
		mid, err := address.IDFromAddress(maddr)
		if err != nil {
			return err
		}

		sb, err := ffiwrapper.New(nil)
		if err != nil {
			return err
		}

		ref := storage.SectorRef{
			ID: abi.SectorID{
				Miner:  abi.ActorID(mid),
				Number: abi.SectorNumber(c2in.SectorNum),
			},
			ProofType: spt(abi.SectorSize(c2in.SectorSize)),
		}

		fmt.Printf("----\nstart proof computation\n")
		start := time.Now()

		proof, err := sb.SealCommit2(context.TODO(), ref, c2in.Phase1Out)
		if err != nil {
			return err
		}

		sealCommit2 := time.Now()

		fmt.Printf("proof: %x\n", proof)

		fmt.Printf("----\nresults (v28) (%d)\n", c2in.SectorSize)
		dur := sealCommit2.Sub(start)

		fmt.Printf("seal: commit phase 2: %s (%s)\n", dur, bps(abi.SectorSize(c2in.SectorSize), 1, dur))
		return nil
	},
}

func bps(sectorSize abi.SectorSize, sectorNum int, d time.Duration) string {
	if d.Nanoseconds() == 0 {
		return "Nan"
	}
	bdata := new(big.Int).SetUint64(uint64(sectorSize))
	bdata = bdata.Mul(bdata, big.NewInt(int64(sectorNum)))
	bdata = bdata.Mul(bdata, big.NewInt(time.Second.Nanoseconds()))
	bps := bdata.Div(bdata, big.NewInt(d.Nanoseconds()))
	return types.SizeStr(types.BigInt{Int: bps}) + "/s"
}

func spt(ssize abi.SectorSize) abi.RegisteredSealProof {
	spt, err := miner.SealProofTypeFromSectorSize(ssize, build.NewestNetworkVersion)
	if err != nil {
		panic(err)
	}

	return spt
}
