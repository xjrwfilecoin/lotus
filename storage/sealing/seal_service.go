package sealing

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/filecoin-project/lotus/node/config"
	"github.com/rcrowley/go-metrics"
	server "github.com/smallnest/rpcx/server"
	"github.com/smallnest/rpcx/serverplugin"
	sectorbuilder "github.com/xjrwfilecoin/go-sectorbuilder"
)

type DummyReader struct {
	limited   int64
	readIndex int64
}

func memsetRepeat(a []byte, v byte) {
	if len(a) == 0 {
		return
	}
	a[0] = v
	for bp := 1; bp < len(a); bp *= 2 {
		copy(a[bp:], a[:bp])
	}
}
func (r DummyReader) Read(p []byte) (n int, err error) {
	if r.limited > 0 {
		remain := r.limited - r.readIndex
		memsetRepeat(p, 0x0e)
		if remain >= int64(len(p)) {

			return len(p), nil
		}
		return int(remain), io.EOF

	}
	memsetRepeat(p, 0x0e)
	return len(p), nil

}
func NewLimitedDummyReader(limited int64) DummyReader {
	reader := DummyReader{
		limited: limited,
	}
	return reader
}
func NewDummyReader(limited int64) DummyReader {
	reader := DummyReader{
		limited: 0,
	}
	return reader
}

type AgentService struct {
	sb       sectorbuilder.Interface
	etcAddrs []string
	ip       string
	port     int
}

func NewAgentService(sb sectorbuilder.Interface, cfg *config.CfgSealAgent) *AgentService {
	agent := &AgentService{
		sb:       sb,
		etcAddrs: cfg.EtcdAddrs,
		ip:       cfg.ServeIP,
		port:     cfg.ServePort,
	}
	go agent.start()
	return agent
}

/*
func pledgeReader(size uint64, parts uint64) io.Reader {
	piece := sectorbuilder.UserBytesForSectorSize((size/127 + size) / parts)

	readers := make([]DummyReader, parts)
	for i := range readers {
		readers[i] = NewLimitedDummyReader(int64(piece))
	}

	return io.MultiReader(readers...)
}*/
func (as *AgentService) AddPiece(ctx context.Context, args *AddPieceArgs, reply *AddPieceReply) error {

	result, err := as.sb.AddPiece(args.Size, args.SectorID, NewLimitedDummyReader(int64(args.Size)), args.Sizes)
	if err == nil {
		reply.PPI = result
	}
	return err
}
func (as *AgentService) addRegistryPlugin(s *server.Server) {

	r := &serverplugin.EtcdRegisterPlugin{
		ServiceAddress: fmt.Sprintf("tcp@%v:%v", as.ip, as.port),
		EtcdServers:    as.etcAddrs,
		BasePath:       basePath,
		Metrics:        metrics.NewRegistry(),
		UpdateInterval: time.Minute,
	}
	err := r.Start()
	if err != nil {
		log.Fatal(err)
	}
	s.Plugins.Add(r)
}

type Args struct {
	A int
	B int
}

type Reply struct {
	C int
}

type Arith int

func (as *AgentService) Mul(ctx context.Context, args *Args, reply *Reply) error {
	reply.C = args.A * args.B
	return nil
}
func (as *AgentService) start() {
	s := server.NewServer()
	as.addRegistryPlugin(s)

	s.RegisterName("AgentService", as, "")

	s.Serve("tcp", fmt.Sprintf("%v:%v", as.ip, as.port))
}

func (as *AgentService) SealPreCommit(ctx context.Context, args *SealPreCommitArgs, reply *SealPreCommitReply) error {
	rspco, err := as.sb.SealPreCommit(ctx, args.SectorID, args.Ticket, args.PPIs)
	if err == nil {
		reply.Rspco = rspco

	}
	return err
}

func (as *AgentService) SealCommit(ctx context.Context, args *SealCommitArgs, reply *SealCommitReply) error {

	ret, err := as.sb.SealCommit(ctx, args.SectorID, args.Ticket, args.Seed, args.PPIs, args.Rspco)
	if err == nil {
		reply.CommR = ret
	}
	return err
}

func (as *AgentService) AccquireSectorID(ctx context.Context, args *AccquireSectorArg, reply *AccquireSectorReply) error {
	reply.SectorID = args.SectorID
	reply.ServerAddr = fmt.Sprintf("tcp@%v:%v", as.ip, as.port) //serveAddr
	return nil
}
