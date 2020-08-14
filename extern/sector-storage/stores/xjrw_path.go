package stores

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/gorilla/mux"
	"golang.org/x/xerrors"
)

func init() {
	//if os.Getenv("BEEGFS_SHD_PATH") == "" {
	//	fmt.Println("BEEGFS_SHD_PATH not set")
	//	panic("")
	//}
	//
	//cmd := "mkdir -p " + filepath.Join(os.Getenv("BEEGFS_SHD_PATH"), "cache")
	//ShellExecute(cmd)
	//cmd = "mkdir -p " + filepath.Join(os.Getenv("BEEGFS_SHD_PATH"), "sealed")
	//ShellExecute(cmd)
	//cmd = "mkdir -p " + filepath.Join(os.Getenv("BEEGFS_SHD_PATH"), "unsealed")
	//ShellExecute(cmd)
	//
	//if os.Getenv("BEEGFS_FINAL_PATH") == "" {
	//	fmt.Println("BEEGFS_FINAL_PATH not set")
	//	panic("")
	//}
	//
	//cmd = "mkdir -p " + filepath.Join(os.Getenv("BEEGFS_FINAL_PATH"), "cache")
	//ShellExecute(cmd)
	//cmd = "mkdir -p " + filepath.Join(os.Getenv("BEEGFS_FINAL_PATH"), "sealed")
	//ShellExecute(cmd)
	//cmd = "mkdir -p " + filepath.Join(os.Getenv("BEEGFS_FINAL_PATH"), "unsealed")
	//ShellExecute(cmd)
	//
	if os.Getenv("REAL_ROOT") == "" {
		fmt.Println("REAL_ROOT not set")
		panic("")
	}

	cmd := "mkdir -p " + os.Getenv("REAL_ROOT")
	ShellExecute(cmd)
}

func GetSectorLocalPath(sector abi.SectorID) SectorPaths {
	return getEnvPath("WORKER_PATH", sector)
}

func GetSectorStoragePath(sector abi.SectorID) SectorPaths {
	return getEnvPath("LOTUS_STORAGE_PATH", sector)
}

func GetSectorBeegfsPath(sector abi.SectorID) SectorPaths {
	return getEnvPath("BEEGFS_SHD_PATH", sector)
}

func GetPreCommit2Task() int {
	num, err := strconv.Atoi(os.Getenv("PRECOMMIT_TASK"))
	if err != nil {
		return 1
	}
	return num
}

func GetSectorBeegfsFinalPath(sector abi.SectorID) SectorPaths {
	return getEnvPath("BEEGFS_FINAL_PATH", sector)
}

func getEnvPath(env string, sector abi.SectorID) SectorPaths {
	rpath := SectorPaths{}
	p := os.Getenv(env)

	rpath.Id = sector
	ft := FTUnsealed
	rpath.Unsealed = filepath.Join(p, ft.String(), SectorName(sector))
	ft = FTCache
	rpath.Cache = filepath.Join(p, ft.String(), SectorName(sector))
	ft = FTSealed
	rpath.Sealed = filepath.Join(p, ft.String(), SectorName(sector))

	return rpath
}

func ShellExecute(cmdStr string) error {
	cmd := exec.Command("/bin/bash", "-c", cmdStr, "|sh")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	err := cmd.Run()
	log.Infof("ShellExecute %s : %v", cmdStr, err)
	return err
}

func (l *Local) MyID() ID {
	for id, v := range l.paths {
		if v.local != "" {
			return id
		}
	}
	log.Error("MyID return empty string")
	return ID("")
}

func (st *Local) PathInfo() (ID, string) {
	p := ""
	var lid ID
	for id, v := range st.paths {
		if v.local != "" {
			p = v.local
			lid = id
			break
		}
	}
	return lid, p
}

func (st *Local) MoveFinalCache(ctx context.Context, sid abi.SectorID, typ SectorFileType) error {
	lid, p := st.PathInfo()

	spath := filepath.Join(p, typ.String(), SectorName(sid))

	cleans := []string{
		fmt.Sprintf("rm -f %s", filepath.Join(spath, "sc-02-data-layer*.dat")),
		fmt.Sprintf("rm -f %s", filepath.Join(spath, "sc-02-data-tree-c*.dat")),
		fmt.Sprintf("rm -f %s", filepath.Join(spath, "sc-02-data-tree-d*.dat")),
	}

	for _, c := range cleans {
		if err := ShellExecute(c); err != nil {
			log.Errorf("clean sector (%v) from [%s]: %+v", sid, c, err)
		}
	}
	topath := filepath.Join(os.Getenv("BEEGFS_FINAL_PATH"), typ.String(), SectorName(sid))

	log.Infof("xjrw move cache %s -> %s", spath, topath)
	cmd := fmt.Sprintf("mv %s %s", spath, topath)
	err := ShellExecute(cmd)
	if err != nil {
		log.Warnf("[xjrw] shellExecute %s err: %v", cmd, err)
		return err
	}
	err = st.index.StorageDropSector(ctx, lid, sid, typ)
	log.Infof("[xjrw] move cache %s -> %s StorageDropSector:%v", spath, topath, err)
	return err
}

func (st *Local) MoveSealCache(ctx context.Context, sid abi.SectorID, typ SectorFileType) error {
	lid, p := st.PathInfo()
	spath := filepath.Join(p, typ.String(), SectorName(sid))
	topath := filepath.Join(os.Getenv("BEEGFS_FINAL_PATH"), typ.String(), SectorName(sid))
	log.Infof("xjrw move cache %s -> %s", spath, topath)
	cmd := fmt.Sprintf("mv %s %s", spath, topath)
	err := ShellExecute(cmd)
	if err != nil {
		log.Warnf("[xjrw] shellExecute %s err: %v", cmd, err)
		return err
	}
	err = st.index.StorageDropSector(ctx, lid, sid, typ)
	log.Infof("[xjrw] move seal %s -> %s StorageDropSector:%v", spath, topath, err)
	return err
}

func (r *Remote) MoveFinalCacheFromRemote(ctx context.Context, url string) error {
	log.Infof("[xjrw] cache Delete %s", url)

	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return xerrors.Errorf("request: %w", err)
	}
	req.Header = r.auth
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return xerrors.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return xerrors.Errorf("non-200 code: %d", resp.StatusCode)
	}

	return nil
}

func (handler *FetchHandler) remoteClearFinalCacheSector(w http.ResponseWriter, r *http.Request) {
	log.Infof("SERVE CLEAR CACHE %s", r.URL)
	vars := mux.Vars(r)

	id, err := ParseSectorID(vars["id"])
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	ft, err := ftFromString(vars["type"])
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}
	if ft&FTCache != 0 {
		err = handler.MoveFinalCache(r.Context(), id, ft)
	} else if ft&FTSealed != 0 {
		err = handler.MoveSealCache(r.Context(), id, ft)
	}
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}
}
