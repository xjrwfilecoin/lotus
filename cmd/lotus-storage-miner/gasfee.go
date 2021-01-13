package main

import (
	"fmt"
	"golang.org/x/xerrors"
	_ "net/http/pprof"
	"strconv"

	"github.com/urfave/cli/v2"

	lcli "github.com/filecoin-project/lotus/cli"
)

var gasCmd = &cli.Command{
	Name:  "gas",
	Usage: "set or get gas fee",
	Subcommands: []*cli.Command{
		setMaxPreCommitGasFee,
		getMaxPreCommitGasFee,
		setMaxCommitGasFee,
		getMaxCommitGasFee,
		setGasFee,
		getGasFee,
	},
}

var setMaxPreCommitGasFee = &cli.Command{
	Name:      "set-maxprecommit",
	Usage:     "Set MaxPreCommitGasFee(unit: FIL)",
	ArgsUsage: "<SetMaxPreCommitGasFee>",
	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)
		if cctx.Args().Len() != 1 {
			return xerrors.Errorf("must input maxprecommit")
		}

		fil := cctx.Args().Get(0)

		fmt.Println("SetMaxPreCommitGasFee ", fil, " FIL")

		return nodeApi.SetMaxPreCommitGasFee(ctx, fil)
	},
}

var getMaxPreCommitGasFee = &cli.Command{
	Name:      "get-maxprecommit",
	Usage:     "Get MaxPreCommitGasFee(unit: FIL)",
	ArgsUsage: "<GetMaxPreCommitGasFee>",
	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)
		MaxPreCommitGasFee, err := nodeApi.GetMaxPreCommitGasFee(ctx)
		gas, err := strconv.ParseFloat(MaxPreCommitGasFee, 64)
		if err != nil {
			return err
		}

		fmt.Println("GetMaxPreCommitGasFee", gas/1e18, " FIL")

		return err
	},
}

var setMaxCommitGasFee = &cli.Command{
	Name:      "set-maxcommit",
	Usage:     "Set MaxCommitGasFee(unit: FIL)",
	ArgsUsage: "<SetMaxCommitGasFee>",
	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)
		if cctx.Args().Len() != 1 {
			return xerrors.Errorf("must input maxcommit")
		}

		fil := cctx.Args().Get(0)

		fmt.Println("SetMaxCommitGasFee", fil, " FIL")

		return nodeApi.SetMaxCommitGasFee(ctx, fil)
	},
}

var getMaxCommitGasFee = &cli.Command{
	Name:      "get-maxcommit",
	Usage:     "Get MaxCommitGasFee(unit: FIL)",
	ArgsUsage: "<GetMaxCommitGasFee>",
	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)
		MaxCommitGasFee, err := nodeApi.GetMaxCommitGasFee(ctx)
		gas, err := strconv.ParseFloat(MaxCommitGasFee, 64)
		if err != nil {
			return err
		}

		fmt.Println("GetMaxCommitGasFee", (gas / 1e18), " FIL")

		return err
	},
}

var setGasFee = &cli.Command{
	Name:      "set-gasfee",
	Usage:     "Set GasFee(unit: nanoFIL)",
	ArgsUsage: "<MaxGasFee>",
	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)
		if cctx.Args().Len() != 1 {
			return xerrors.Errorf("must input gas")
		}

		fil := cctx.Args().Get(0)

		fmt.Println("setGasFee ", fil, " nFIL")

		return nodeApi.SetGasFee(ctx, fil)
	},
}

var getGasFee = &cli.Command{
	Name:      "get-gasfee",
	Usage:     "Get GasFee",
	ArgsUsage: "<GetGasFee>",
	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)
		GasFee, err := nodeApi.GetGasFee(ctx)
		if err != nil {
			return err
		}
		fmt.Println("GetGasFee ", GasFee)

		return nil
	},
}
