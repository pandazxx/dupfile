package dupfile

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
)

type GoProfile struct {
	cpuFilePath string
	memFilePath string
}

func NewProfile() *GoProfile {
	ret := GoProfile{}
	flag.StringVar(&ret.cpuFilePath, "cpu-profile-file", "", "Path of cpu profile result file")
	flag.StringVar(&ret.memFilePath, "mem-profile-file", "", "Path of mem profile result file")
	return &ret
}

func (g *GoProfile) Start() {
	if g.cpuFilePath != "" {
		f, err := os.Open(g.cpuFilePath)
		if err != nil {
			if os.IsNotExist(err) {
				f, err = os.Create(g.cpuFilePath)
				if err != nil {
					panic(fmt.Sprintf("Error in creating file %v: %v", g.cpuFilePath, err))
				}
			} else {
				panic(fmt.Sprintf("Error in opening file %v: %v", g.cpuFilePath, err))
			}
		}
		fmt.Println("Starting cpu profile")
		pprof.StartCPUProfile(f)
	}
}

func (g *GoProfile) End() {
	if g.cpuFilePath != "" {
		fmt.Println("Stoping cpu profile")
		pprof.StopCPUProfile()
	}
}
