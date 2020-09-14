package stores

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"math/bits"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	gopath "path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/fsutil"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/lotus/extern/sector-storage/tarutil"

	"github.com/hashicorp/go-multierror"
	files "github.com/ipfs/go-ipfs-files"
	"golang.org/x/xerrors"
)

var FetchTempSubdir = "fetching"

const DEF_CACHE = 12
const DEF_IP = "172.70"

type Remote struct {
	local *Local
	index SectorIndex
	auth  http.Header

	limit chan struct{}

	fetchLk  sync.Mutex
	fetching map[abi.SectorID]chan struct{}
}

var localIP = ""

func ShellExecute(cmdStr string) error {
	cmd := exec.Command("/bin/bash", "-c", cmdStr, "|sh")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	err := cmd.Run()
	log.Infof("ShellExecute %s : %v", cmdStr, err)
	return err
}

func getLocalIP() string {
	if localIP != "" {
		return localIP
	}

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}

	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil && strings.Contains(ipnet.IP.String(), DEF_IP) {
				localIP = ipnet.IP.String()
				log.Infof("ip: %v", localIP)
				return localIP
			}
		}
	}
	return ""
}
func (r *Remote) RemoveCopies(ctx context.Context, s abi.SectorID, types SectorFileType) error {
	// TODO: do this on remotes too
	//  (not that we really need to do that since it's always called by the
	//   worker which pulled the copy)

	return r.local.RemoveCopies(ctx, s, types)
}

func NewRemote(local *Local, index SectorIndex, auth http.Header, fetchLimit int) *Remote {
	return &Remote{
		local: local,
		index: index,
		auth:  auth,

		limit: make(chan struct{}, fetchLimit),

		fetching: map[abi.SectorID]chan struct{}{},
	}
}

func ReadTXT(fileName string) []string {
	files := []string{}
	log.Infof("getTXTLine %v", fileName)
	f, err := os.Open(fileName)
	if err != nil {
		return []string{}
	}
	buf := bufio.NewReader(f)
	for {
		line, err := buf.ReadString('\n')
		line = strings.TrimSpace(line)
		if line != "" {
			files = append(files, line)
		}
		if err != nil {
			if err == io.EOF {
				return files
			}
			return []string{}
		}
	}
	return []string{}
}

func JudgeCacheComplete(cache string) bool {
	file := cache + ".txt"
	lines := ReadTXT(file)
	if len(lines) < DEF_CACHE {
		return false
	}

	lines = lines[len(lines)-DEF_CACHE:]

	mapData := make(map[string]struct{})
	for _, file := range lines {
		mapData[file] = struct{}{}
	}

	if len(mapData) == DEF_CACHE {
		return true
	}
	return false
}

func (r *Remote) AcquireSector(ctx context.Context, s abi.SectorID, spt abi.RegisteredSealProof, existing SectorFileType, allocate SectorFileType, pathType PathType, op AcquireMode) (SectorPaths, SectorPaths, error) {
	if existing|allocate != existing^allocate {
		return SectorPaths{}, SectorPaths{}, xerrors.New("can't both find and allocate a sector")
	}

	for {
		r.fetchLk.Lock()

		c, locked := r.fetching[s]
		if !locked {
			r.fetching[s] = make(chan struct{})
			r.fetchLk.Unlock()
			break
		}

		r.fetchLk.Unlock()

		select {
		case <-c:
			continue
		case <-ctx.Done():
			return SectorPaths{}, SectorPaths{}, ctx.Err()
		}
	}

	defer func() {
		r.fetchLk.Lock()
		close(r.fetching[s])
		delete(r.fetching, s)
		r.fetchLk.Unlock()
	}()

	paths, stores, err := r.local.AcquireSector(ctx, s, spt, existing, allocate, pathType, op)
	if err != nil {
		return SectorPaths{}, SectorPaths{}, xerrors.Errorf("local acquire error: %w", err)
	}

	var toFetch SectorFileType
	for _, fileType := range PathTypes {
		if fileType&existing == 0 {
			continue
		}

		if PathByType(paths, fileType) == "" {
			toFetch |= fileType
		}
	}

	apaths, ids, err := r.local.AcquireSector(ctx, s, spt, FTNone, toFetch, pathType, op)
	if err != nil {
		return SectorPaths{}, SectorPaths{}, xerrors.Errorf("allocate local sector for fetching: %w", err)
	}

	odt := FSOverheadSeal
	if pathType == PathStorage {
		odt = FsOverheadFinalized
	}

	releaseStorage, err := r.local.Reserve(ctx, s, spt, toFetch, ids, odt)
	if err != nil {
		return SectorPaths{}, SectorPaths{}, xerrors.Errorf("reserving storage space: %w", err)
	}
	defer releaseStorage()

	for _, fileType := range PathTypes {
		if fileType&existing == 0 {
			continue
		}

		if PathByType(paths, fileType) != "" {
			continue
		}

		dest := PathByType(apaths, fileType)
		storageID := PathByType(ids, fileType)

		if _, err := os.Stat(dest); err != nil || existing != FTSealed|FTCache {
			log.Infof("not exist dest %v %v %v", dest, existing, err)
			url, err := r.acquireFromRemote(ctx, s, fileType, dest)
			if err != nil {
				return SectorPaths{}, SectorPaths{}, err
			}

			if op == AcquireMove {
				if err := r.deleteFromRemote(ctx, url); err != nil {
					log.Warnf("deleting sector %v from %s (delete %s): %+v", s, storageID, url, err)
				}
			}
		} else {
			log.Infof("exist dest %v %v", dest, existing)
		}

		SetPathByType(&paths, fileType, dest)
		SetPathByType(&stores, fileType, storageID)

		if err := r.index.StorageDeclareSector(ctx, ID(storageID), s, fileType, op == AcquireMove); err != nil {
			log.Warnf("declaring sector %v in %s failed: %+v", s, storageID, err)
			continue
		}
	}

	return paths, stores, nil

}

func (r *Remote) FetchRemoveRemote(ctx context.Context, s abi.SectorID, typ SectorFileType) error {
	si, err := r.index.StorageFindSector(ctx, s, typ, 0, false)
	if err != nil {
		return xerrors.Errorf("finding existing sector %d(t:%d) failed: %w", s, typ, err)
	}

	var merr error
	for _, info := range si {
		for _, url := range info.URLs {
			log.Infof("url  = %v", url)
			if !strings.Contains(url, getLocalIP()) && strings.Contains(url, DEF_IP) {
				dest := ""
				if typ == FTSealed {
					dest = filepath.Join(os.Getenv("WORKER_PATH"), "sealed")
				} else if typ == FTCache {
					dest = filepath.Join(os.Getenv("WORKER_PATH"), "cache")
				} else {
					return xerrors.Errorf(" %v not exist type", s)
				}
				dest = filepath.Join(dest, SectorName(s))

				if _, err := os.Stat(dest); err != nil || (err == nil && typ == FTCache && !JudgeCacheComplete(dest)) {
					tempDest, err := tempFetchDest(dest, true)
					if err != nil {
						return err
					}

					if err := os.RemoveAll(dest); err != nil {
						return xerrors.Errorf("removing dest: %w", err)
					}

					err = r.fetch(ctx, url, tempDest)
					if err != nil {
						merr = multierror.Append(merr, xerrors.Errorf("fetch error %s (storage %s) -> %s: %w", url, info.ID, tempDest, err))
						continue
					}

					if err := move(tempDest, dest); err != nil {
						return xerrors.Errorf("fetch move error (storage %s) %s -> %s: %w", info.ID, tempDest, dest, err)
					}
				}

				if err := r.deleteFromRemote(ctx, url); err != nil {
					log.Warnf("remove %s: %+v", url, err)
					continue
				}
				return nil
			}
		}
	}

	return nil
}

func tempFetchDest(spath string, create bool) (string, error) {
	st, b := filepath.Split(spath)
	tempdir := filepath.Join(st, FetchTempSubdir)
	if create {
		if err := os.MkdirAll(tempdir, 0755); err != nil { // nolint
			return "", xerrors.Errorf("creating temp fetch dir: %w", err)
		}
	}

	return filepath.Join(tempdir, b), nil
}

func (r *Remote) acquireFromRemote(ctx context.Context, s abi.SectorID, fileType SectorFileType, dest string) (string, error) {
	si, err := r.index.StorageFindSector(ctx, s, fileType, 0, false)
	if err != nil {
		return "", err
	}

	if len(si) == 0 {
		return "", xerrors.Errorf("failed to acquire sector %v from remote(%d): %w", s, fileType, storiface.ErrSectorNotFound)
	}

	sort.Slice(si, func(i, j int) bool {
		return si[i].Weight < si[j].Weight
	})

	var merr error
	for _, info := range si {
		// TODO: see what we have local, prefer that

		for _, url := range info.URLs {
			tempDest, err := tempFetchDest(dest, true)
			if err != nil {
				return "", err
			}

			if err := os.RemoveAll(dest); err != nil {
				return "", xerrors.Errorf("removing dest: %w", err)
			}

			err = r.fetch(ctx, url, tempDest)
			if err != nil {
				merr = multierror.Append(merr, xerrors.Errorf("fetch error %s (storage %s) -> %s: %w", url, info.ID, tempDest, err))
				continue
			}

			if err := move(tempDest, dest); err != nil {
				return "", xerrors.Errorf("fetch move error (storage %s) %s -> %s: %w", info.ID, tempDest, dest, err)
			}

			if merr != nil {
				log.Warnw("acquireFromRemote encountered errors when fetching sector from remote", "errors", merr)
			}
			return url, nil
		}
	}

	return "", xerrors.Errorf("failed to acquire sector %v from remote (tried %v): %w", s, si, merr)
}

func (r *Remote) fetch(ctx context.Context, url, outname string) error {
	log.Infof("Fetch %s -> %s", url, outname)

	if len(r.limit) >= cap(r.limit) {
		log.Infof("Throttling fetch, %d already running", len(r.limit))
	}

	// TODO: Smarter throttling
	//  * Priority (just going sequentially is still pretty good)
	//  * Per interface
	//  * Aware of remote load
	select {
	case r.limit <- struct{}{}:
		defer func() { <-r.limit }()
	case <-ctx.Done():
		return xerrors.Errorf("context error while waiting for fetch limiter: %w", ctx.Err())
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return xerrors.Errorf("request: %w", err)
	}
	req.Header = r.auth
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return xerrors.Errorf("do request: %w", err)
	}
	defer resp.Body.Close() // nolint

	if resp.StatusCode != 200 {
		return xerrors.Errorf("non-200 code: %d", resp.StatusCode)
	}

	/*bar := pb.New64(w.sizeForType(typ))
	bar.ShowPercent = true
	bar.ShowSpeed = true
	bar.Units = pb.U_BYTES

	barreader := bar.NewProxyReader(resp.Body)

	bar.Start()
	defer bar.Finish()*/

	mediatype, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		return xerrors.Errorf("parse media type: %w", err)
	}

	if err := os.RemoveAll(outname); err != nil {
		return xerrors.Errorf("removing dest: %w", err)
	}

	switch mediatype {
	case "application/x-tar":
		return tarutil.ExtractTar(resp.Body, outname)
	case "application/octet-stream":
		return files.WriteTo(files.NewReaderFile(resp.Body), outname)
	default:
		return xerrors.Errorf("unknown content type: '%s'", mediatype)
	}
}

func (r *Remote) MoveStorage(ctx context.Context, s abi.SectorID, spt abi.RegisteredSealProof, types SectorFileType) error {
	// Make sure we have the data local
	_, _, err := r.AcquireSector(ctx, s, spt, types, FTNone, PathStorage, AcquireMove)
	if err != nil {
		return xerrors.Errorf("acquire src storage (remote): %w", err)
	}

	return r.local.MoveStorage(ctx, s, spt, types)
}

func (r *Remote) Remove(ctx context.Context, sid abi.SectorID, typ SectorFileType, force bool) error {
	if bits.OnesCount(uint(typ)) != 1 {
		return xerrors.New("delete expects one file type")
	}

	if err := r.local.Remove(ctx, sid, typ, force); err != nil {
		return xerrors.Errorf("remove from local: %w", err)
	}

	si, err := r.index.StorageFindSector(ctx, sid, typ, 0, false)
	if err != nil {
		return xerrors.Errorf("finding existing sector %d(t:%d) failed: %w", sid, typ, err)
	}

	for _, info := range si {
		for _, url := range info.URLs {
			if err := r.deleteFromRemote(ctx, url); err != nil {
				log.Warnf("remove %s: %+v", url, err)
				continue
			}
			break
		}
	}

	return nil
}

func (r *Remote) deleteFromRemote(ctx context.Context, url string) error {
	log.Infof("Delete %s", url)

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return xerrors.Errorf("request: %w", err)
	}
	req.Header = r.auth
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return xerrors.Errorf("do request: %w", err)
	}
	defer resp.Body.Close() // nolint

	if resp.StatusCode != 200 {
		return xerrors.Errorf("non-200 code: %d", resp.StatusCode)
	}

	return nil
}

func (r *Remote) FsStat(ctx context.Context, id ID) (fsutil.FsStat, error) {
	st, err := r.local.FsStat(ctx, id)
	switch err {
	case nil:
		return st, nil
	case errPathNotFound:
		break
	default:
		return fsutil.FsStat{}, xerrors.Errorf("local stat: %w", err)
	}

	si, err := r.index.StorageInfo(ctx, id)
	if err != nil {
		return fsutil.FsStat{}, xerrors.Errorf("getting remote storage info: %w", err)
	}

	if len(si.URLs) == 0 {
		return fsutil.FsStat{}, xerrors.Errorf("no known URLs for remote storage %s", id)
	}

	rl, err := url.Parse(si.URLs[0])
	if err != nil {
		return fsutil.FsStat{}, xerrors.Errorf("failed to parse url: %w", err)
	}

	rl.Path = gopath.Join(rl.Path, "stat", string(id))

	req, err := http.NewRequest("GET", rl.String(), nil)
	if err != nil {
		return fsutil.FsStat{}, xerrors.Errorf("request: %w", err)
	}
	req.Header = r.auth
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fsutil.FsStat{}, xerrors.Errorf("do request: %w", err)
	}
	switch resp.StatusCode {
	case 200:
		break
	case 404:
		return fsutil.FsStat{}, errPathNotFound
	case 500:
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fsutil.FsStat{}, xerrors.Errorf("fsstat: got http 500, then failed to read the error: %w", err)
		}

		return fsutil.FsStat{}, xerrors.Errorf("fsstat: got http 500: %s", string(b))
	}

	var out fsutil.FsStat
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fsutil.FsStat{}, xerrors.Errorf("decoding fsstat: %w", err)
	}

	defer resp.Body.Close() // nolint

	return out, nil
}

var _ Store = &Remote{}
