package sealing

import (
	ffi "github.com/filecoin-project/filecoin-ffi"
	sectorbuilder "github.com/filecoin-project/go-sectorbuilder"
)

type AddPieceArgs struct {
	Size     uint64
	SectorID uint64
	Sizes    []uint64
}
type FreeWorkersArg struct {
	Client      string
	FreeWorkers int
}
type WorkerArgs struct {
	Client     string
	Workstates sectorbuilder.WorkerStats
}
type NoneReply struct {
	success bool
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
type ScrubArgs struct {
	SectorID uint64
	CommR    [ffi.CommitmentBytesLen]byte
}
type ScrubReply struct {
	SectorID uint64

	Err error
}
type AccquireSectorArg struct {
	SectorID uint64
}

type AccquireSectorReply struct {
	SectorID   uint64
	ServerAddr string
}

const (
	basePath        = "/SealAgent"
	agentClientAddr = ":33223"
	serveAddr       = ":33222"
)
