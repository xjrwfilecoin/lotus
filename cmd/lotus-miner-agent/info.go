package main

import (
	"context"
	"fmt"

	"gopkg.in/urfave/cli.v2"

	"github.com/filecoin-project/lotus/api"
	lcli "github.com/filecoin-project/lotus/cli"
)

var infoCmd = &cli.Command{
	Name:  "info",
	Usage: "Print storage miner info",
	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := lcli.GetMinerAgentAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)
		// TODO: indicate whether the post worker is in use
		wstat, err := nodeApi.WorkerStats(ctx)
		if err != nil {
			return err
		}

		fmt.Printf("Worker use:\n")
		fmt.Printf("\tLocal: %d / %d (+%d reserved)\n", wstat.LocalTotal-wstat.LocalReserved-wstat.LocalFree, wstat.LocalTotal-wstat.LocalReserved, wstat.LocalReserved)
		fmt.Printf("\tRemote: %d / %d\n", wstat.RemotesTotal-wstat.RemotesFree, wstat.RemotesTotal)

		fmt.Printf("Queues:\n")
		fmt.Printf("\tAddPiece: %d\n", wstat.AddPieceWait)
		fmt.Printf("\tPreCommit: %d\n", wstat.PreCommitWait)
		fmt.Printf("\tCommit: %d\n", wstat.CommitWait)
		fmt.Printf("\tUnseal: %d\n", wstat.UnsealWait)
		fmt.Printf("\tFreeCommittee: %d\n", wstat.FreeCommittee)
		fmt.Printf("\tFreePreCommittee: %d\n", wstat.FreePreCommittee)
		fmt.Printf("\tPendingCommit: %d\n", wstat.PendingCommit)

		// TODO: grab actr state / info
		//  * Sealed sectors (count / bytes)
		//  * Power
		return nil
	},
}

func sectorsInfo(ctx context.Context, napi api.StorageMiner) (map[string]int, error) {
	sectors, err := napi.SectorsList(ctx)
	if err != nil {
		return nil, err
	}

	out := map[string]int{
		"Total": len(sectors),
	}
	for _, s := range sectors {
		st, err := napi.SectorsStatus(ctx, s)
		if err != nil {
			return nil, err
		}

		out[api.SectorStates[st.State]]++
	}

	return out, nil
}
