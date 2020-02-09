package sealing

import sectorbuilder "github.com/xjrwfilecoin/go-sectorbuilder"

type AddPieceArgs struct {
	Size     uint64
	SectorID uint64
	Sizes    []uint64
}

type AddPieceReply struct {
	PPI sectorbuilder.PublicPieceInfo
}
type SealCommitArgs struct {
	SectorID uint64
	Ticket   sectorbuilder.SealTicket
	Seed     sectorbuilder.SealSeed
	PPIs     []sectorbuilder.PublicPieceInfo
	Rspco    sectorbuilder.RawSealPreCommitOutput
}
type SealCommitReply struct {
	CommR []byte
}

type SealPreCommitArgs struct {
	SectorID uint64
	Ticket   sectorbuilder.SealTicket
	PPIs     []sectorbuilder.PublicPieceInfo
}
type SealPreCommitReply struct {
	Rspco sectorbuilder.RawSealPreCommitOutput
}

type AccquireSectorArg struct {
	SectorID uint64
}

type AccquireSectorReply struct {
	SectorID   uint64
	ServerAddr string
}

type ServiceConfig struct {
	ServeIP   string
	ServePort int
	EtcdAddrs []string
}

const (
	basePath        = "/SealAgent"
	agentClientAddr = ":33223"
	serveAddr       = ":33222"
)
