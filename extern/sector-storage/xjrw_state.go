package sectorstorage

import (
	"database/sql"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	_ "github.com/mattn/go-sqlite3"
	"sync"
	"time"
)

type SectorState struct {
	Start  string
	End    string
	Worker string
}

var smu sync.Mutex
var db *sql.DB = nil
var state = map[string]map[sealtasks.TaskType]*SectorState{}

func initState() {
	var err error
	db, err = sql.Open("sqlite3", "./sector.db")
	if err != nil {
		panic(err)
		return
	}

	sqlStmt := `
	create table if not exists sector (id VARCHAR(64) not null primary key, addpiecestart text,addpieceend text,addpiecehost text,p1start text,p1end text,p1host text,p2start text,p2end text,p2host text, c1start text,c1end text,c1host text, c2start text,c2end text,c2host text);
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		panic(err)
		return
	}

	RemoveFile("res_mngr.lock")
}

func startSector(sector, wk string, tk sealtasks.TaskType) {
	smu.Lock()
	defer smu.Unlock()

	_, ok := state[sector]
	if !ok {
		state[sector] = map[sealtasks.TaskType]*SectorState{}
	}
	_, ok = state[sector][tk]
	if !ok {
		state[sector][tk] = &SectorState{
			Worker: wk,
		}
	}
	state[sector][tk].Start = time.Now().Format(time.RFC3339)

	tx, err := db.Begin()
	if err != nil {
		log.Errorf("sqllite begin %v", err)
	}

	pre := ""
	if tk == sealtasks.TTAddPiece {
		pre = "replace into sector(id, addpiecestart,addpiecehost) values(?, ?, ?)"
	} else if tk == sealtasks.TTPreCommit1 {
		pre = "update sector set p1start = ?, p1host = ?  where id = ?"
	} else if tk == sealtasks.TTPreCommit2 {
		pre = "update sector set p2start = ? ,p2host = ? where id = ?"
	} else if tk == sealtasks.TTCommit1 {
		pre = "update sector set c1start = ? ,c1host = ? where id = ?"
	} else if tk == sealtasks.TTCommit2 {
		pre = "update sector set c2start = ? ,c2host = ? where id = ?"
	} else {
		log.Errorf("not support type")
		return
	}

	stmt, err := tx.Prepare(pre)
	if err != nil {
		log.Errorf("sqllite prepare %v", err)
	}
	defer stmt.Close()
	if tk == sealtasks.TTAddPiece {
		_, err = stmt.Exec(sector, state[sector][tk].Start, wk)
	} else {
		_, err = stmt.Exec(state[sector][tk].Start, wk, sector)
	}

	if err != nil {
		log.Errorf("sqllite exec %v", err)
	}
	tx.Commit()
}

func endSector(sector, wk string, tk sealtasks.TaskType) {
	smu.Lock()
	defer smu.Unlock()

	_, ok := state[sector]
	if !ok {
		state[sector] = map[sealtasks.TaskType]*SectorState{}
	}
	_, ok = state[sector][tk]
	if !ok {
		state[sector][tk] = &SectorState{
			Worker: wk,
		}
	}
	state[sector][tk].End = time.Now().Format(time.RFC3339)

	tx, err := db.Begin()
	if err != nil {
		log.Errorf("sqllite begin %v", err)
	}

	pre := ""
	if tk == sealtasks.TTAddPiece {
		pre = "update sector set addpieceend = ? where id = ?"
	} else if tk == sealtasks.TTPreCommit1 {
		pre = "update sector set p1end = ? where id = ?"
	} else if tk == sealtasks.TTPreCommit2 {
		pre = "update sector set p2end = ? where id = ?"
	} else if tk == sealtasks.TTCommit1 {
		pre = "update sector set c1end = ? where id = ?"
	} else if tk == sealtasks.TTCommit2 {
		pre = "update sector set c2end = ? where id = ?"
	} else {
		log.Errorf("not support type")
		return
	}

	stmt, err := tx.Prepare(pre)
	if err != nil {
		log.Errorf("sqllite prepare %v", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(state[sector][tk].End, sector)
	if err != nil {
		log.Errorf("sqllite exec %v", err)
	}
	tx.Commit()
}

func findSector(sector string, tk sealtasks.TaskType) string {
	smu.Lock()
	defer smu.Unlock()

	_, ok := state[sector][tk]
	if !ok {
		que := ""
		if tk == sealtasks.TTAddPiece {
			que = "select addpiecehost from sector where id = "
		} else if tk == sealtasks.TTPreCommit1 {
			que = "select p1host from sector where id = "
		} else if tk == sealtasks.TTPreCommit2 {
			que = "select p2host from sector where id = "
		} else if tk == sealtasks.TTCommit1 {
			que = "select c1host from sector where id = "
		} else if tk == sealtasks.TTCommit2 {
			que = "select c2host from sector where id = "
		} else {
			log.Errorf("not support type")
			return ""
		}
		que += "'" + sector + "'"

		rows, err := db.Query(que)
		if err != nil {
			log.Errorf("sqllite query %v %v ", err, que)
		}

		defer rows.Close()
		for rows.Next() {
			var host string
			err = rows.Scan(&host)
			if err != nil {
				log.Errorf("sqllite scan %v %v", err, que)
			}
			return host
		}
		return ""
	}

	return state[sector][tk].Worker
}
