package storage

import (
	"context"
	"github.com/filecoin-project/lotus/chain/types"
	proof2 "github.com/filecoin-project/specs-actors/v2/actors/runtime/proof"
	"io"
	"math/rand"
	"time"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"

	sealing "github.com/filecoin-project/lotus/extern/storage-sealing"
)

// TODO: refactor this to be direct somehow

func (m *Miner) Address() address.Address {
	return m.sealing.Address()
}

func (m *Miner) AddPieceToAnySector(ctx context.Context, size abi.UnpaddedPieceSize, r io.Reader, d sealing.DealInfo) (abi.SectorNumber, abi.PaddedPieceSize, error) {
	return m.sealing.AddPieceToAnySector(ctx, size, r, d)
}

func (m *Miner) StartPackingSector(sectorNum abi.SectorNumber) error {
	return m.sealing.StartPacking(sectorNum)
}

func (m *Miner) ListSectors() ([]sealing.SectorInfo, error) {
	return m.sealing.ListSectors()
}

func (m *Miner) GetSectorInfo(sid abi.SectorNumber) (sealing.SectorInfo, error) {
	return m.sealing.GetSectorInfo(sid)
}

func (m *Miner) PledgeSector() error {
	return m.sealing.PledgeSector()
}

func (m *Miner) ForceSectorState(ctx context.Context, id abi.SectorNumber, state sealing.SectorState) error {
	return m.sealing.ForceSectorState(ctx, id, state)
}

func (m *Miner) SetMaxPreCommitGasFee(ctx context.Context, maxPreCommit types.FIL) error {
	return m.sealing.SetMaxPreCommitGasFee(ctx, maxPreCommit)
}

func (m *Miner) GetMaxPreCommitGasFee(ctx context.Context) (string, error) {
	return m.sealing.GetMaxPreCommitGasFee(ctx)
}

func (m *Miner) SetMaxCommitGasFee(ctx context.Context, maxCommit types.FIL) error {
	return m.sealing.SetMaxCommitGasFee(ctx, maxCommit)
}

func (m *Miner) GetMaxCommitGasFee(ctx context.Context) (string, error) {
	return m.sealing.GetMaxCommitGasFee(ctx)
}

func (m *Miner) SetGasFee(ctx context.Context, gas types.FIL) error {
	return m.sealing.SetGasFee(ctx, gas)
}

func (m *Miner) GetGasFee(ctx context.Context) (string, error) {
	return m.sealing.GetGasFee(ctx)
}

func (m *Miner) RefreshConf(ctx context.Context) (string, error) {
	return m.sealing.RefreshConf(ctx)
}

func (m *Miner) WindowsPost(ctx context.Context, sectorInfo []proof2.SectorInfo, random string) error {
	mid, err := address.IDFromAddress(m.maddr)
	if err != nil {
		return err
	}

	randomness := make([]byte, abi.RandomnessLength)
	rand.Seed(time.Now().Unix())
	for i := 0; i < abi.RandomnessLength; i++ {
		randomness[i] = byte(rand.Intn(256))
	}

	if random != "" {
		randomness = abi.PoStRandomness(random)
	}

	log.Info("WindowsPost random ", randomness)
	m.sealer.GenerateWindowPoSt(ctx, abi.ActorID(mid), sectorInfo, randomness)
	return nil
}

func (m *Miner) WinningPost(ctx context.Context, sectorInfo []proof2.SectorInfo, number int) error {
	mid, err := address.IDFromAddress(m.maddr)
	if err != nil {
		return err
	}

	m.sealer.GenerateWinningPoSt(ctx, abi.ActorID(mid), sectorInfo, abi.PoStRandomness{0, 9, 2, 7, 6, 5, 4, 3, 2, 1, 0, 9, 8, 7, 6, 45, 3, 2, 1, 0, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 9, 7})
	return nil
}

func (m *Miner) RemoveSector(ctx context.Context, id abi.SectorNumber) error {
	return m.sealing.Remove(ctx, id)
}

func (m *Miner) TerminateSector(ctx context.Context, id abi.SectorNumber) error {
	return m.sealing.Terminate(ctx, id)
}

func (m *Miner) TerminateFlush(ctx context.Context) (*cid.Cid, error) {
	return m.sealing.TerminateFlush(ctx)
}

func (m *Miner) TerminatePending(ctx context.Context) ([]abi.SectorID, error) {
	return m.sealing.TerminatePending(ctx)
}

func (m *Miner) MarkForUpgrade(id abi.SectorNumber) error {
	return m.sealing.MarkForUpgrade(id)
}

func (m *Miner) IsMarkedForUpgrade(id abi.SectorNumber) bool {
	return m.sealing.IsMarkedForUpgrade(id)
}
