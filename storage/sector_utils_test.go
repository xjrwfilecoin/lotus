package storage

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	sectorbuilder "github.com/xjrwfilecoin/go-sectorbuilder"
)

func testFill(t *testing.T, n uint64, exp []uint64) {
	f, err := fillersFromRem(n)
	assert.NoError(t, err)
	assert.Equal(t, exp, f)

	var sum uint64
	for _, u := range f {
		sum += u
	}
	assert.Equal(t, n, sum)
}

func TestFillersFromRem(t *testing.T) {
	for i := 8; i < 32; i++ {
		// single
		ub := sectorbuilder.UserBytesForSectorSize(uint64(1) << i)
		testFill(t, ub, []uint64{ub})

		// 2
		ub = sectorbuilder.UserBytesForSectorSize(uint64(5) << i)
		ub1 := sectorbuilder.UserBytesForSectorSize(uint64(1) << i)
		ub3 := sectorbuilder.UserBytesForSectorSize(uint64(4) << i)
		testFill(t, ub, []uint64{ub1, ub3})

		// 4
		ub = sectorbuilder.UserBytesForSectorSize(uint64(15) << i)
		ub2 := sectorbuilder.UserBytesForSectorSize(uint64(2) << i)
		ub4 := sectorbuilder.UserBytesForSectorSize(uint64(8) << i)
		testFill(t, ub, []uint64{ub1, ub2, ub3, ub4})

		// different 2
		ub = sectorbuilder.UserBytesForSectorSize(uint64(9) << i)
		testFill(t, ub, []uint64{ub1, ub4})
	}

}

func TestPieceCommments(t *testing.T) {

	size := 1024
	//检查文件是否存在
	dataFileName := fmt.Sprintf("/tmp/file%d", size)
	commFileName := fmt.Sprintf("/tmp/comm%d", size)
	if _, err := os.Stat(dataFileName); os.IsNotExist(err) {
		// path/to/whatever does not exist
		randReader := io.LimitReader(rand.New(rand.NewSource(42)), int64(size))
		buffer := make([]byte, size)
		randReader.Read(buffer)
		ioutil.WriteFile(dataFileName, buffer, os.ModePerm)
	}

	if _, err := os.Stat(commFileName); os.IsNotExist(err) {
		// path/to/whatever does not exist
	}
	//return [ffi.CommitmentBytesLen]byte{}, nil
	file1, err := os.Open(dataFileName)

	if err != nil {
		fmt.Println(fmt.Sprintf("open file failed:%v", err))
	}
	commP3, err3 := sectorbuilder.GeneratePieceCommitment(file1, uint64(size))

	commP4, err4 := sectorbuilder.GeneratePieceCommitment(file1, uint64(size))
	if err3 != nil || err4 != nil {
		fmt.Println(fmt.Sprintf("commit failed:%v, %v", err3, err4))
	}

	if bytes.Compare(commP3[:], commP4[:]) == 0 {
		fmt.Println("OK")
	} else {
		fmt.Println(fmt.Sprintf("commP3: %v", commP3[:]))
		fmt.Println(fmt.Sprintf("commP4: %v", commP4[:]))
		fmt.Println("error")
	}

	commP5, err3 := sectorbuilder.AddPieces(file1, uint64(size))
}
