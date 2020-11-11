package stores

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/extern/sector-storage/tarutil"
)

var log = logging.Logger("stores")

type FetchHandler struct {
	*Local
}

func (handler *FetchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) { // /remote/
	mux := mux.NewRouter()

	mux.HandleFunc("/remote/stat/{id}", handler.remoteStatFs).Methods("GET")
	mux.HandleFunc("/remote/{type}/{id}", handler.remoteGetSector).Methods("GET")
	mux.HandleFunc("/remote/{type}/{id}/{file}", handler.remoteGetSectorEx).Methods("GET")
	mux.HandleFunc("/remote/{type}/{id}", handler.remoteDeleteSector).Methods("DELETE")

	mux.ServeHTTP(w, r)
}

func (handler *FetchHandler) remoteStatFs(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := ID(vars["id"])

	st, err := handler.Local.FsStat(r.Context(), id)
	switch err {
	case errPathNotFound:
		w.WriteHeader(404)
		return
	case nil:
		break
	default:
		w.WriteHeader(500)
		log.Errorf("%+v", err)
		return
	}

	if err := json.NewEncoder(w).Encode(&st); err != nil {
		log.Warnf("error writing stat response: %+v", err)
	}
}

func (handler *FetchHandler) remoteGetSectorEx(w http.ResponseWriter, r *http.Request) {
	log.Infof("SERVE GET %s", r.URL)
	vars := mux.Vars(r)

	log.Infof("remoteGetSector1 %v", vars)
	id, err := ParseSectorID(vars["id"])
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}
	log.Infof("remoteGetSector2 %v", id)

	ft, err := ftFromString(vars["type"])
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}
	log.Infof("remoteGetSector3 %v", ft)

	file, err := strconv.Atoi(vars["file"])
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}
	log.Infof("remoteGetSector3 %v", file)

	// The caller has a lock on this sector already, no need to get one here

	// passing 0 spt because we don't allocate anything
	paths, _, err := handler.Local.AcquireSector(r.Context(), id, 0, ft, FTNone, PathStorage, AcquireMove)
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}
	log.Infof("remoteGetSector4 %v", paths)

	// TODO: reserve local storage here

	path := PathByType(paths, ft)
	if path == "" {
		log.Error("acquired path was empty")
		w.WriteHeader(500)
		return
	}
	log.Infof("remoteGetSector5 %v", path)

	deleteFiles(file, path)

	rd, err := tarutil.TarDirectory(path)
	w.Header().Set("Content-Type", "application/x-tar")
	log.Infof("remoteGetSector7 %v %v", rd, err)

	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	log.Infof("remoteGetSector8 %v %v", rd, w)
	w.WriteHeader(200)
	if _, err := io.Copy(w, rd); err != nil { // TODO: default 32k buf may be too small
		log.Error("%+v", err)
		return
	}
}

func (handler *FetchHandler) remoteGetSector(w http.ResponseWriter, r *http.Request) {
	log.Infof("SERVE GET %s", r.URL)
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

	// The caller has a lock on this sector already, no need to get one here

	// passing 0 spt because we don't allocate anything
	paths, _, err := handler.Local.AcquireSector(r.Context(), id, 0, ft, FTNone, PathStorage, AcquireMove)
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	// TODO: reserve local storage here

	path := PathByType(paths, ft)
	if path == "" {
		log.Error("acquired path was empty")
		w.WriteHeader(500)
		return
	}

	stat, err := os.Stat(path)
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	var rd io.Reader
	if stat.IsDir() {
		rd, err = tarutil.TarDirectory(path)
		w.Header().Set("Content-Type", "application/x-tar")
	} else {
		rd, err = os.OpenFile(path, os.O_RDONLY, 0644) // nolint
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	if err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}

	w.WriteHeader(200)
	if _, err := io.Copy(w, rd); err != nil { // TODO: default 32k buf may be too small
		log.Error("%+v", err)
		return
	}
}

func (handler *FetchHandler) remoteDeleteSector(w http.ResponseWriter, r *http.Request) {
	log.Infof("SERVE DELETE %s", r.URL)
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

	if err := handler.Remove(r.Context(), id, ft, false); err != nil {
		log.Error("%+v", err)
		w.WriteHeader(500)
		return
	}
}

func ftFromString(t string) (SectorFileType, error) {
	switch t {
	case FTUnsealed.String():
		return FTUnsealed, nil
	case FTSealed.String():
		return FTSealed, nil
	case FTCache.String():
		return FTCache, nil
	default:
		return 0, xerrors.Errorf("unknown sector file type: '%s'", t)
	}
}

func deleteFiles(file int, path string) {
	log.Infof("SERVE deleteFiles %v %v", path, file)
	if file&0x1 != 0 {
		daleteFile(filepath.Join(path, "sc-02-data-layer-1.dat"))
	}
	if file&0x2 != 0 {
		daleteFile(filepath.Join(path, "sc-02-data-layer-2.dat"))
	}
	if file&0x4 != 0 {
		daleteFile(filepath.Join(path, "sc-02-data-layer-3.dat"))
	}
	if file&0x8 != 0 {
		daleteFile(filepath.Join(path, "sc-02-data-layer-4.dat"))
	}
	if file&0x10 != 0 {
		daleteFile(filepath.Join(path, "sc-02-data-layer-5.dat"))
	}
	if file&0x20 != 0 {
		daleteFile(filepath.Join(path, "sc-02-data-layer-6.dat"))
	}
	if file&0x40 != 0 {
		daleteFile(filepath.Join(path, "sc-02-data-layer-7.dat"))
	}
	if file&0x80 != 0 {
		daleteFile(filepath.Join(path, "sc-02-data-layer-8.dat"))
	}
	if file&0x100 != 0 {
		daleteFile(filepath.Join(path, "sc-02-data-layer-9.dat"))
	}
	if file&0x200 != 0 {
		daleteFile(filepath.Join(path, "sc-02-data-layer-10.dat"))
	}
	if file&0x400 != 0 {
		daleteFile(filepath.Join(path, "sc-02-data-layer-11.dat"))
	}
	if file&0x800 != 0 {
		daleteFile(filepath.Join(path, "sc-02-data-tree-d.dat"))
	}
}

func daleteFile(file string) {
	if err := os.Remove(file); err != nil {
		log.Errorf("remove file (%v): %+v", file, err)
	}
}
