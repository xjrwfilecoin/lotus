package storage

import (
	"context"
	"github.com/filecoin-project/lotus/chain/types"
	"io"

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

func (m *Miner) RemoveSector(ctx context.Context, id abi.SectorNumber) error {
	return m.sealing.Remove(ctx, id)
}

func (m *Miner) MarkForUpgrade(id abi.SectorNumber) error {
	return m.sealing.MarkForUpgrade(id)
}

func (m *Miner) IsMarkedForUpgrade(id abi.SectorNumber) bool {
	return m.sealing.IsMarkedForUpgrade(id)
}
