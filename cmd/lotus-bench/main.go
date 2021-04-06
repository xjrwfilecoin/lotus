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
	"syscall"
	"time"

	"github.com/docker/go-units"
	sectorstorage "github.com/filecoin-project/lotus/extern/sector-storage"
	logging "github.com/ipfs/go-log/v2"
	//"github.com/mitchellh/go-homedir"
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

func SingleProcess(sigfile string) (*os.File, error) {
	f, err := os.OpenFile(sigfile, os.O_RDONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		log.Error("bench already start: ", err)
		return nil, err
	}

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		log.Error("bench already start: ", err)
		return nil, err
	}

	if err := ioutil.WriteFile(sigfile, []byte(fmt.Sprintln(os.Getpid())), os.ModePerm); err != nil {
		log.Error("bench already start: ", err)
		return nil, err
	}
	return f, nil
}

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
			Value: true,
		},
		&cli.BoolFlag{
			Name:  "only-p1",
			Usage: "only do ap&p1 of the benchmark",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "only-p2",
			Usage: "only do p2 of the benchmark",
			Value: false,
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
			Name:  "p1-limit",
			Usage: "Parallel P1 numbers  at the same time",
			Value: 4,
		},
		&cli.IntFlag{
			Name:  "p2-limit",
			Usage: "Parallel P2 numbers  at the same time",
			Value: 1,
		},
		&cli.IntFlag{
			Name:  "parallel",
			Usage: "num run in parallel",
			Value: 1,
		},
	},
	Action: func(c *cli.Context) error {
		sbdir := c.String("storage-dir")

		log.Info("storage-dir: ", sbdir)

		if err := os.MkdirAll(os.TempDir(), 0775); err != nil {
			return err
		}

		f, err := SingleProcess(filepath.Join(os.TempDir(), "bench.lock"))
		defer f.Close()
		if err != nil {
			return err
		}

		// sector size
		sectorSizeInt, err := units.RAMInBytes(c.String("sector-size"))
		if err != nil {
			return err
		}
		sectorSize := abi.SectorSize(sectorSizeInt)

		sbfs := &basicfs.Provider{
			Root: sbdir,
		}

		sb, err := ffiwrapper.New(sbfs)
		if err != nil {
			return err
		}

		onlyp1 := c.Bool("only-p1")
		onlyp2 := c.Bool("only-p2")
		p1limit := c.Int("p1-limit")
		p2limit := c.Int("p2-limit")

		log.Infof("para: %v %v %v %v", onlyp1, onlyp2, p1limit, p2limit)

		if onlyp1 && onlyp2 {
			return xerrors.Errorf("only-p1 and only-p2 cannot be true at the same time")
		}

		if os.Getenv("FIL_PROOFS_SSD_PARENT") == "" {
			panic("FIL_PROOFS_SSD_PARENT not set")
		}
		sectorstorage.ShellExecute("rm -rf " + filepath.Join(os.Getenv("FIL_PROOFS_SSD_PARENT"), "*"))

		if err := os.MkdirAll(filepath.Join(sbdir, "cache"), 0775); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Join(sbdir, "unsealed"), 0775); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Join(sbdir, "sealed"), 0775); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Join(sbdir, "undo"), 0775); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Join(sbdir, "faults"), 0775); err != nil {
			return err
		}

		destFile := strconv.FormatInt(time.Now().UnixNano()/1e6, 10) + ".json"
		sectorstorage.ShellExecute("mv " + filepath.Join(filepath.Join(sbdir, "undo"), "backup.json") + " " + filepath.Join(filepath.Join(sbdir, "faults"), destFile))

		err = runSeals(sb, sectorSize, p1limit, p2limit, sbdir, onlyp1, onlyp2)
		if err != nil {
			return xerrors.Errorf("failed to run seals: %w", err)
		}

		stop := make(chan struct{})
		<-stop

		return nil
	},
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

func ReadJson(fileName string) (map[string]string, error) {
	state := make(map[string]string)
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Info("read json err: ", err)
		return map[string]string{}, err
	}

	err = json.Unmarshal(data, &state)
	if err != nil {
		log.Info("Unmarshal json err : ", err)
		return map[string]string{}, err
	}
	return state, nil
}

func WriteJson(id string, sbdir string) error {
	if id != "" {
		tasks.Delete(id)
	}

	state := make(map[string]string)

	tasks.Range(func(k, v interface{}) bool {
		state[k.(string)] = v.(string)
		return true
	})

	file := filepath.Join(filepath.Join(sbdir, "undo"), "backup.json")
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

func deletefiles(id abi.SectorID, sbdir string) {
	cachePath := filepath.Join(sbdir, "cache")
	destPath := filepath.Join(cachePath, storiface.SectorName(id))
	sectorstorage.ShellExecute("rm -rf " + filepath.Join(destPath, "sc-02-data-tree-r-last*"))
	return
	cachePath = filepath.Join(sbdir, "cache")
	destPath = filepath.Join(cachePath, storiface.SectorName(id))
	sectorstorage.ShellExecute("rm -rf " + destPath)

	sealedPath := filepath.Join(sbdir, "sealed")
	destPath = filepath.Join(sealedPath, storiface.SectorName(id))
	sectorstorage.ShellExecute("rm -rf " + destPath)

	unsealedPath := filepath.Join(sbdir, "unsealed")
	destPath = filepath.Join(unsealedPath, storiface.SectorName(id))
	sectorstorage.ShellExecute("rm -rf " + destPath)
}

func runSeals(sb *ffiwrapper.Sealer, sectorSize abi.SectorSize, p1limit int, p2limit int, sbdir string, onlyp1 bool, onlyp2 bool) error {
	var rP1 sync.Map
	var rP2 sync.Map
	var r sync.Map
	ids := make(map[string]string)

	go func() {
		for {
			if len(ids) == 0 {
				filesPath := scanDir(filepath.Join(sbdir, "faults"))
				for _, path := range filesPath {
					log.Info("read file ", path)
					if state, err := ReadJson(path); err == nil {
						for id, random := range state {
							if sid, err := storiface.ParseSectorID(id); err == nil {
								deletefiles(sid, sbdir)
								ids[id] = random
								tasks.Store(id, random)
							} else {
								log.Errorf("%v parse sector : %v",  id, err)
							}
						}
						os.Remove(path)
						log.Info("remove file ", path)
						break
					} else {
						log.Info("file err: ", err)
					}
				}

				if len(ids) > 0 {
					WriteJson("", sbdir)
				}
			}

			waitlist := fmt.Sprintf("wait task %v: ", len(ids))
			for id, _ := range ids {
				waitlist = waitlist + id + ", "
			}
			log.Info(waitlist)

			for id, random := range ids {
				length := 0
				rP1.Range(func(k, v interface{}) bool {
					length++
					return true
				})

				if length < p1limit {
					delete(ids, id)
					rP1.Store(id, random)
					go func(id string, random string) error {
						defer rP1.Delete(id)

						if _id, err := storiface.ParseSectorID(id); err == nil {
							sid := storage.SectorRef{
								ID: _id,
								ProofType: spt(sectorSize),
							}
							log.Info("p1 start ", id)

							size := abi.PaddedPieceSize(sectorSize).Unpadded()
							pi, err := sb.AddPiece(context.TODO(), sid, nil, size, sealing.NewNullReader(size))
							if err != nil {
								log.Infof("p1 AddPiece failed: %v %v", id, err)
								return err
							}

							sDec, err := base64.StdEncoding.DecodeString(random)
							if err != nil {
								log.Infof("p1 DecodeString failed: %v %v", id, err)
								return err
							}

							p1out, err := sb.SealPreCommit1(context.TODO(), sid, sDec, []abi.PieceInfo{pi})
							if err != nil {
								log.Infof("p1 failed %v : %v", id, err)
								return xerrors.Errorf("commit: %w", err)
							}

							rP2.Store(id, p1out)

							log.Infof("p1 finish: %v", id)
						} else {
							log.Errorf("%v parse sector : %v",  id, err)
						}

						return nil
					}(id, random)
				}
			}
			<-time.After(time.Second * 5)
		}
	}()

	go func() {
		for {
			rP2.Range(func(k, v interface{}) bool {
				id := k.(string)
				p1out := v.(storage.PreCommit1Out)

				length := 0
				r.Range(func(k, v interface{}) bool {
					length++
					return true
				})

				if length < p2limit {
					rP2.Delete(k)
					r.Store(id, p1out)

					go func(id string, p1out storage.PreCommit1Out) error {
						defer r.Delete(id)

						if _id, err := storiface.ParseSectorID(id); err == nil {
							sid := storage.SectorRef{
								ID: _id,
								ProofType: spt(sectorSize),
							}

							log.Info("p2 start ", id)
							_, err := sb.SealPreCommit2(context.TODO(), sid, p1out)
							if err != nil {
								log.Infof("p2 failed %v : %v", id, err)
								return err
							}

							WriteJson(id, sbdir)

							log.Info("p2 finish ", id)
							cachePath := filepath.Join(sbdir, "cache")
							destPath := filepath.Join(cachePath, storiface.SectorName(sid.ID))
							sectorstorage.ShellExecute("rm -rf " + filepath.Join(destPath, "sc-02-data-tree-c*"))
							sectorstorage.ShellExecute("rm -rf " + filepath.Join(destPath, "sc-02-data-tree-d*"))
							sectorstorage.ShellExecute("rm -rf " + filepath.Join(destPath, "sc-02-data-layer*"))

							unsealedPath := filepath.Join(sbdir, "unsealed")
							destPath = filepath.Join(unsealedPath, storiface.SectorName(sid.ID))
							sectorstorage.ShellExecute("rm -rf " + destPath)
						} else {
							log.Errorf("%v parse sector : %v",  id, err)
						}

						return nil
					}(id, p1out)
				}

				return true
			})

			<-time.After(time.Second * 5)
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
