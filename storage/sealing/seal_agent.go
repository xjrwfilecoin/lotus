package sealing

import (
	"context"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"sync"

	"github.com/filecoin-project/lotus/node/config"
	"github.com/ipfs/go-datastore"
	ktds "github.com/ipfs/go-datastore/keytransform"
	client "github.com/smallnest/rpcx/client"
	sectorbuilder "github.com/xjrwfilecoin/go-sectorbuilder"
	"golang.org/x/xerrors"
)

type BySectorIdSelector struct {
	ds *ktds.Datastore
}

func (s *BySectorIdSelector) Select(ctx context.Context, servicePath, serviceMethod string, args interface{}) string {

	obj := reflect.ValueOf(args)

	elem := obj.Elem()
	sectorID := elem.FieldByName("sectorID")
	if sectorID.Kind() == reflect.Uint64 {
		secID := sectorID.Uint()
		if secID != 0 {
			result, ok := s.ds.Get(datastore.NewKey(strconv.FormatUint(secID, 10)))
			if ok == nil {
				return string(result) //.(string)
			}
		}
	}
	return ""
}

func (s *BySectorIdSelector) UpdateServer(servers map[string]string) {

}

func (s *BySectorIdSelector) Update(sectorID uint64, server string) {
	err := xerrors.Errorf("persist data store error")
	if s.ds != nil {
		err = s.ds.Put(datastore.NewKey(strconv.FormatUint(sectorID, 10)), []byte(server))
	}

	if err != nil {
		log.Errorf("Error in update sector store info")
	}
}

var _ sectorbuilder.Interface = &SealAgent{}

type SealAgent struct {
	sb sectorbuilder.Interface
	//注册服务器的地址
	discovery   client.ServiceDiscovery
	freeWorkers sync.Map
	ip          string
	port        int
	selector    *BySectorIdSelector
}

func (sa *SealAgent) getSectorDealer(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "Hello, world!\n")
}

func NewSealAgent(sb sectorbuilder.Interface, cfg *config.CfgSealAgent, ds *ktds.Datastore) *SealAgent {

	sa := &SealAgent{
		sb:        sb,
		discovery: client.NewEtcdDiscovery(basePath, "AgentService", cfg.EtcdAddrs, nil),
		//	freeWorkers:make( map[string]int)
		ip:       cfg.ServeIP,
		port:     cfg.ServePort,
		selector: &BySectorIdSelector{ds},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/sector", sa.getSectorDealer)

	go http.ListenAndServe("127.0.0.1:33221", mux)
	//创建一个更新sectorID的服务

	return sa
}
func (sa SealAgent) RateLimit() func() {
	return func() {
	}
}
func (sa SealAgent) AddPiece(size uint64, sectorID uint64, reader io.Reader, sizes []uint64) (sectorbuilder.PublicPieceInfo, error) {
	return sectorbuilder.PublicPieceInfo{}, xerrors.Errorf("Not implemented")
}
func (sa SealAgent) AddRemotePiece(size uint64, sectorID uint64, sizes []uint64) (sectorbuilder.PublicPieceInfo, error) {
	xclient := client.NewXClient("AgentService", client.Failtry, client.SelectByUser, sa.discovery, client.DefaultOption)
	defer xclient.Close()
	args := &AddPieceArgs{
		Size:     size,
		SectorID: sectorID,
		Sizes:    sizes,
	}
	reply := &AddPieceReply{}
	err := xclient.Call(context.Background(), "AddPiece", args, reply)
	if err == nil {

		return reply.PPI, nil
	}
	return sectorbuilder.PublicPieceInfo{}, err

}
func (sa SealAgent) SectorSize() uint64 {
	return sa.sb.SectorSize()
}

func (sa SealAgent) Scrub(ssInfo sectorbuilder.SortedPublicSectorInfo) []*sectorbuilder.Fault {

	return sa.sb.Scrub(ssInfo)
}
func (sa SealAgent) GetFreeWorkers() int {
	freeNum := int(0)
	sa.freeWorkers.Range(func(key, val interface{}) bool {
		freeNum += val.(int)
		return true
	})
	return freeNum
}

func (sa SealAgent) Busy() bool {
	return false
}
func (sa SealAgent) GenerateEPostCandidates(sectorInfo sectorbuilder.SortedPublicSectorInfo, challengeSeed [sectorbuilder.CommLen]byte, faults []uint64) ([]sectorbuilder.EPostCandidate, error) {
	return sa.sb.GenerateEPostCandidates(sectorInfo, challengeSeed, faults)
}
func (sa SealAgent) GenerateFallbackPoSt(sbppinfo sectorbuilder.SortedPublicSectorInfo, commP [sectorbuilder.CommLen]byte, faults []uint64) ([]sectorbuilder.EPostCandidate, []byte, error) {
	return sa.sb.GenerateFallbackPoSt(sbppinfo, commP, faults)
}
func (sa SealAgent) ComputeElectionPoSt(sectorInfo sectorbuilder.SortedPublicSectorInfo, challengeSeed []byte, winners []sectorbuilder.EPostCandidate) ([]byte, error) {
	return sa.sb.ComputeElectionPoSt(sectorInfo, challengeSeed, winners)
}
func (sa SealAgent) SealPreCommit(ctx context.Context, sectorID uint64, ticket sectorbuilder.SealTicket, ppi []sectorbuilder.PublicPieceInfo) (sectorbuilder.RawSealPreCommitOutput, error) {
	xclient := client.NewXClient("AgentService", client.Failtry, client.SelectByUser, sa.discovery, client.DefaultOption)
	defer xclient.Close()
	args := &SealPreCommitArgs{
		SectorID: sectorID,
		Ticket:   ticket,
		PPIs:     ppi,
	}
	reply := &SealPreCommitReply{}
	err := xclient.Call(ctx, "SealPreCommit", args, reply)
	if err == nil {
		return reply.Rspco, nil
	}
	return sectorbuilder.RawSealPreCommitOutput{}, err
}
func (sa SealAgent) SealCommit(ctx context.Context, sectorId uint64, ticket sectorbuilder.SealTicket, seed sectorbuilder.SealSeed, ppi []sectorbuilder.PublicPieceInfo, rspco sectorbuilder.RawSealPreCommitOutput) ([]byte, error) {
	xclient := client.NewXClient("AgentService", client.Failtry, client.SelectByUser, sa.discovery, client.DefaultOption)
	defer xclient.Close()
	args := &SealCommitArgs{
		SectorID: sectorId,
		Ticket:   ticket,
		Seed:     seed,
		PPIs:     ppi,
		Rspco:    rspco,
	}
	replay := &SealCommitReply{}
	err := xclient.Call(ctx, "SealCommit", args, replay)
	if err == nil {
		return replay.CommR, nil
	}
	return nil, err
}

//这个需要改造成一个tcpReader
func (sa SealAgent) ReadPieceFromSealedSector(sectorID uint64, offset uint64, size uint64, ticket []byte, commD []byte) (io.ReadCloser, error) {
	return nil, xerrors.Errorf("not implemented")
}
func (sa SealAgent) GetPath(typename string, sname string) (string, error) {
	return sa.sb.GetPath(typename, sname)
}
func (sa SealAgent) WorkerStats() sectorbuilder.WorkerStats {
	return sa.sb.WorkerStats()
}
func (sa SealAgent) AddWorker(context.Context, sectorbuilder.WorkerCfg) (<-chan sectorbuilder.WorkerTask, error) {
	return nil, xerrors.Errorf("not implemented")
}
func (sa SealAgent) TaskDone(context.Context, uint64, sectorbuilder.SealRes) error {
	return xerrors.Errorf("not implemented")
}
func (sa SealAgent) UpdateSectorInfo(context.Context, uint64, sectorbuilder.SealRes) error {
	return xerrors.Errorf("not implemented")
}

type WorkerInfoArgs struct {
	AgentID    string
	FreeWorker int
}
type WorkerInfoReply struct {
	err error
}

func (sa SealAgent) UpdateWorkerInfo(ctx context.Context, args WorkerInfoArgs, reply WorkerInfoReply) error {
	sa.freeWorkers.Store(args.AgentID, args.FreeWorker)
	reply.err = nil
	return nil
}

func (sa SealAgent) AcquireSectorId() (uint64, error) {
	//生成一个xclient，使用随机的方法获得一个结果
	var sectorID uint64
	var err error
	if sa.sb != nil {
		sectorID, err = sa.sb.AcquireSectorId()
	}

	xclient := client.NewXClient("AgentService", client.Failtry, client.RandomSelect, sa.discovery, client.DefaultOption)
	defer xclient.Close()
	args := &AccquireSectorArg{
		SectorID: sectorID,
	}
	reply := &AccquireSectorReply{}
	err = xclient.Fork(context.Background(), "AccquireSectorID", args, reply)
	if err == nil {
		//sa.freeWorkers.Store(reply.SectorID, reply.ServerAddr)
		sa.selector.Update(reply.SectorID, reply.ServerAddr)
		return reply.SectorID, nil
	}
	return 0, err
}
