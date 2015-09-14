package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/pandazxx/dupfile/profile"
	"github.com/pandazxx/gostat"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	PEEK_BYTES                    = 1024
	MAX_GO_ROUTINE                = 8
	WALK_FILE_COUNT_STAT_KEY      = "filecount"
	WALK_FILE_DURATION_STAT_KEY   = "filewalkdur"
	MAIN_EXE_TIME_STAT_KEY        = "maintime"
	WORKER_DURATION_STAT_KEY      = "workertime"
	WEAK_KEY_COUNT_STAT_KEY       = "weakkeycount"
	WEAK_KEY_TIME_SPEND_STAT_KEY  = "weakkeytimespend"
	FILL_INFO_COUNT_STAT_KEY      = "fillinfocount"
	FILL_INFO_TIME_SPEND_STAT_KEY = "fillinfotimespend"
)

var (
	statistics = gostat.NewStat()
)

type fileInfo struct {
	Path      string
	Sha1      []byte
	Size      uint64
	HeadBytes []byte
	TailBytes []byte
}

func (f *fileInfo) WeakKey() uint64 {
	h := sha1.New()
	w := bufio.NewWriter(h)
	w.Write(f.HeadBytes)
	w.WriteByte(0)
	w.Write(f.TailBytes)
	w.WriteByte(0)
	binary.Write(w, binary.LittleEndian, f.Size)
	w.Flush()
	key := h.Sum(nil)
	var ret uint64
	binary.Read(bytes.NewBuffer(key[:]), binary.LittleEndian, &ret)
	return ret
}

func (f *fileInfo) Fill() {
	// fmt.Println("Filling file " + f.Path)
	var headBytes uint64
	headBytes = PEEK_BYTES
	if f.Size < PEEK_BYTES {
		headBytes = f.Size
	}
	inf, err := os.Open(f.Path)
	if err != nil {
		// Error handle
		fmt.Println("error in opening " + f.Path)
		return
	}
	defer inf.Close()
	f.HeadBytes = make([]byte, headBytes)
	_, err = inf.Read(f.HeadBytes)
	// fmt.Println(f.HeadBytes)
	if err != nil {
		// Error handle
		fmt.Println("Error in reading file " + f.Path)
		return
	}
	// fmt.Println(f)
}

func dirWalker(rootPath string) map[int64][]fileInfo {
	sizeIndex := make(map[int64][]fileInfo)
	statistics.DurationBegin(WALK_FILE_DURATION_STAT_KEY)
	defer statistics.DurationEnd(WALK_FILE_DURATION_STAT_KEY)
	filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// error handle
			return nil
		}
		if info.Mode()&os.ModeType != 0 {
			// pass non-regular files
			return nil
		}

		fInfo := fileInfo{
			Path: path,
			Size: uint64(info.Size()),
		}
		infoList, found := sizeIndex[info.Size()]
		if !found {
			infoList = make([]fileInfo, 0, 5)
		}
		sizeIndex[info.Size()] = append(infoList, fInfo)
		statistics.AddCount(WALK_FILE_COUNT_STAT_KEY, 1)

		return nil
	})
	return sizeIndex
}

func diffWorker(inCh <-chan []fileInfo, strongCompare bool) <-chan []fileInfo {
	ret := make(chan []fileInfo)
	statSession := statistics.NewSession()
	defer statSession.End()
	go func() {
		weakKeyIndex := make(map[uint64][]fileInfo)
		defer close(ret)
		for fileList := range inCh {
			for _, info := range fileList {
				statSession.DurationBegin(FILL_INFO_TIME_SPEND_STAT_KEY)
				info.Fill()
				statSession.DurationEnd(FILL_INFO_TIME_SPEND_STAT_KEY)
				statSession.AddCount(FILL_INFO_COUNT_STAT_KEY, 1)

				statSession.DurationBegin(WEAK_KEY_TIME_SPEND_STAT_KEY)
				key := info.WeakKey()
				statSession.DurationEnd(WEAK_KEY_TIME_SPEND_STAT_KEY)
				statSession.AddCount(WEAK_KEY_COUNT_STAT_KEY, 1)
				list, found := weakKeyIndex[key]
				if !found {
					list = make([]fileInfo, 0, 2)
				}
				weakKeyIndex[key] = append(list, info)
			}
		}
		for _, infos := range weakKeyIndex {
			if len(infos) > 1 && !strongCompare {
				ret <- infos
			} else {
				// Not supported yet
			}
		}
	}()

	return ret
}

func merge(cs []<-chan []fileInfo) <-chan []fileInfo {
	var wg sync.WaitGroup
	out := make(chan []fileInfo)

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan []fileInfo) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func findDupFiles(rootPath string) [][]fileInfo {
	sizeIndex := dirWalker(rootPath)
	inCh := make(chan []fileInfo, MAX_GO_ROUTINE*2)
	outChs := make([]<-chan []fileInfo, 0, MAX_GO_ROUTINE)
	for i := 0; i < MAX_GO_ROUTINE; i++ {
		outChs = append(outChs, diffWorker(inCh, false))
	}

	for _, infos := range sizeIndex {
		if len(infos) > 1 {
			inCh <- infos
		}
	}
	close(inCh)

	ret := make([][]fileInfo, 0, 10)
	for dupInfos := range merge(outChs) {
		ret = append(ret, dupInfos)
	}

	return ret
}

var startDir string
var profile *dupfile.GoProfile

func init() {
	const (
		defaultStartDir = "./"
		startDirDesc    = "Specifying the root dir we start looking for duplicat files"
	)
	flag.StringVar(&startDir, "start-dir", defaultStartDir, startDirDesc)
	flag.StringVar(&startDir, "s", defaultStartDir, startDirDesc+" (shorthand)")
	profile = dupfile.NewProfile()
}

type sortBySize [][]fileInfo

func (s sortBySize) Len() int {
	return len(s)
}

func (s sortBySize) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortBySize) Less(i, j int) bool {
	return s[i][0].Size < s[j][0].Size
}

func main() {
	flag.Parse()
	profile.Start()
	defer profile.End()
	statistics.DurationBegin(MAIN_EXE_TIME_STAT_KEY)
	result := findDupFiles(startDir)
	sort.Sort(sort.Reverse(sortBySize(result)))
	for _, infos := range result {
		// fmt.Printf("Duplicate files size[%v]:\n", infos[0].Size)
		for _, info := range infos {
			fmt.Printf("%20d\t%X\t%v\n", info.Size, info.WeakKey(), info.Path)
		}
	}
	statistics.DurationEnd(MAIN_EXE_TIME_STAT_KEY)
	walkDuration := statistics.GetDuration(WALK_FILE_DURATION_STAT_KEY)
	walkFileCount := statistics.GetCount(WALK_FILE_COUNT_STAT_KEY)

	fmt.Printf("%d files walked through in %v\n", walkFileCount, walkDuration)
	fmt.Printf("Total time spend %v\n", statistics.GetDuration(MAIN_EXE_TIME_STAT_KEY))

	fmt.Printf("%d file info filled in %v\n", statistics.GetCount(FILL_INFO_COUNT_STAT_KEY), statistics.GetDuration(FILL_INFO_TIME_SPEND_STAT_KEY))
	fmt.Printf("%d weak keys generated in %v\n", statistics.GetCount(WEAK_KEY_COUNT_STAT_KEY), statistics.GetDuration(WEAK_KEY_TIME_SPEND_STAT_KEY))
}
