package stores

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"strings"
)

const DEF_CACHE = 12

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

func init() {
	fmt.Println("localIP: ", getLocalIP())
}

func getLocalIP() string {
	if localIP != "" {
		return localIP
	}

	name, err := os.Hostname()
	if err != nil {
		return ""
	}

	addrs, err := net.LookupHost(name)
	if err != nil {
		return ""
	}

	for _, addr := range addrs {
		if !strings.Contains(addr, "127.0") {
			localIP = addr
			return localIP
		}
	}

	//addrs, err := net.InterfaceAddrs()
	//if err != nil {
	//	return ""
	//}
	//
	//for _, address := range addrs {
	//	if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
	//		if ipnet.IP.To4() != nil && strings.Contains(ipnet.IP.String(), DEF_IP) {
	//			localIP = ipnet.IP.String()
	//			log.Infof("ip: %v", localIP)
	//			return localIP
	//		}
	//	}
	//}
	panic("getLocalIP error ")
	return ""
}

func ReadTXT(fileName string) []string {
	files := []string{}
	log.Infof("getTXTLine %v", fileName)
	f, err := os.Open(fileName)
	if err != nil {
		log.Errorf("not find %v :%v", fileName, err)
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
	return files
}

func JudgeCacheComplete(cache string) bool {
	file := cache + ".txt"
	lines := ReadTXT(file)
	if len(lines) < DEF_CACHE || !strings.Contains(lines[len(lines)-1], "layer-11") {
		log.Errorf(" %v is wrong ", file)
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

func appendToFile(fileName string, content string) error {
	// 以只写的模式，打开文件
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf("not open %v : %v ", fileName, err)
	} else {
		n, _ := f.Seek(0, os.SEEK_END)
		_, err = f.WriteAt([]byte(content), n)
	}
	defer f.Close()
	return err
}

func WriteTXT(cache string) error {
	file := cache + ".txt"
	log.Infof("write to %v", file)
	contents := []string{"sc-02-data-tree-d.dat\n", "sc-02-data-layer-1.dat\n", "sc-02-data-layer-2.dat\n", "sc-02-data-layer-3.dat\n", "sc-02-data-layer-4.dat\n", "sc-02-data-layer-5.dat\n",
		"sc-02-data-layer-6.dat\n", "sc-02-data-layer-7.dat\n", "sc-02-data-layer-8.dat\n", "sc-02-data-layer-9.dat\n", "sc-02-data-layer-10.dat\n", "sc-02-data-layer-11.dat\n"}
	for _, context := range contents {
		if err := appendToFile(file, context); err != nil {
			return err
		}
	}
	return nil
}

func ScanDir(dirName string) int {
	files, err := ioutil.ReadDir(dirName)
	if err != nil {
		log.Errorf("not open %v : %v", dirName, err)
		return -1
	}
	return len(files)
}
