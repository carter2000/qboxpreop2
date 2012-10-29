package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"qbox.us/cc/config"
	"qbox.us/log"
)

var (
	confName = flag.String("t", "preop.conf", "task file")
)

const (
	logPrefix = "data_"
)

type task struct {
	IODomain	string	`json:"io_domain"`
	LogIndex	int	`json:"log_index"`
	Params		[]string`json:"params"`
	Off		int64	`json:"off"`
	IntervalNS	int64	`json:"interval_ns"`
	MaxLogSize	int64	`json:"max_logsize`
	Follow		bool	`json:"follow"`
	Save		bool	`json:"save"`
	Debug		int	`json:"debug_level"`
	RW		sync.RWMutex	`json:"-"`
}

func main() {

	flag.Parse()

	var conf task
	if err := config.LoadEx(&conf, *confName); err != nil {
		return
	}

	log.SetOutputLevel(conf.Debug)

	logName := logPrefix + strconv.Itoa(conf.LogIndex)
	logFile, err := os.Open(logName)
	if err != nil {
		log.Fatal("open", conf.Log, "fail:", err)
	}
	defer func() {
		logFile.Close()
	}()

	defer save(&conf)
	go interruptSave(&conf)
	go successiveSave(&conf)

	logFile.Seek(conf.Off, os.SEEK_SET)
	reader := bufio.NewReader(logFile)

	line := make([]byte, 0, 128)
	now := time.Now()
	last := now
	for {
		now = time.Now()
		sleep(now, last, conf.IntervalNS)
		last = now

		b, _, err := reader.ReadLine()
		line = append(line, b...)

		if err != nil {
			if !conf.Follow {
				break
			}

			if newFile, ok := reOpen(logFile, conf.MaxLogSize); ok {
				logFile.Close()
				if logFile.Name() != newFile.Name() {
					conf.RW.Lock()
					conf.Off = 0
					conf.LogIndex = getLogIndex(newFile.Name())
					conf.RW.Unlock()
				}
				logFile = newFile
				logFile.Seek(conf.Off, os.SEEK_SET)
				reader = bufio.NewReader(logFile)
			}

			continue
		}

		atomic.AddInt64(&conf.Off, int64(len(line)) + 1)

		if uri, ok := parseLine(string(line); ok {
			preOp(conf.IODomain, string(uri), conf.Params, conf.Save)
		}

		line = line[:0]
	}
}

func getLogIndex(name string) int {

	if !strings.HasPrefix(name, logPrefix) {
		return -1
	}

	i, err := strconv.Atoi(name[len(logPrefix):])
	if err != nil {
		return -1
	}

	return i
}

func getNextLog(name string) *os.File {

	dir, err := os.Open("./")
	if err != nil {
		return nil
	}

	fns, err := dir.Readdirnames(0)
	if err != nil {
		return nil
	}

	closestF := nil
	closestIdx := -1
	minIdx := getLogIndex(name)
	for _, fn := range fns {
		idx := GetLogIndex(fn.Name())
		if idx <= minIdx {
			continue
		}

		f, err := os.Open(fn)
		if err != nil {
			continue
		}

		if closestF != nil && closetIdx < idx {
			continue
		}

		if closestF != nil {
			closestF.Close()
		}

		closestF = f
		closestIdx = idx
	}

	return closestF
}

func sleep(now, last time.Time, tick int64) {

	elapse := now.Sub(last).Nanoseconds()
	if tick > elapse {
		time.Sleep(time.Duration(tick - elapse))
	}
}

func reOpen(f *os.File, maxSize int64) (*os.File, bool) {

	oldFi, err := f.Stat()
	if err != nil {
		log.Warn("stat", err)
		return oldF, false
	}

	if oldFi.Size() >= maxSize {
		if nf := getNextLog(f); nf != nil {
			return nf, true
		}
		return f, false
	}

	newFi, err := os.Stat(f.Name())
	if err != nil {
		log.Warn("stat", err)
		return oldF, false
	}

	if newFi.Size() == oldFi.Size() {
		return oldF, false
	}

	nf, err := os.Open(f.Name())
	if err != nil {
		log.Warn("open", f.Name(), "fail:", err)
	}

	return nf, true
}

func save(t *task) {

	t.RW.RLock()
	b, err := json.MarshalIndent(t, "", "\t")
	t.RW.RUnlock()
	if err != nil {
		log.Warn("marshal:", err)
		return
	}

	tmpName := *confName + "_"
	if err := ioutil.WriteFile(tmpName, b, 0666); err != nil {
		log.Warn("write:", err)
		return
	}

	if err := os.Rename(tmpName, *confName); err != nil {
		log.Warn("rename:", err)
		return
	}
	return
}

func interruptSave(t *task) {

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	for _ = range c {
		save(t)
		os.Exit(0)
	}
}

func successiveSave(t *task) {

	c := time.Tick(time.Minute)
	for _ = range c {
		save(t)
	}
}

func preOp(host string, uri string, params []string, save bool) {


	for i,p := range params {
		url := host + "/" + uri + p
		log.Debug("url:", url)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Warn("http newrequest fail:", url, err)
			continue
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Warn("http get fail:", url, err)
			continue
		}

		if save {
			saveResult(uri+strconv.Itoa(i), resp.Body)
		}
		resp.Body.Close()
	}
}

func saveResult(name string, reader io.Reader) {

	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	f, err := os.Create(name)
	if err != nil {
		log.Debug("create:", name, err)
		return
	}

	io.Copy(f, reader)

	f.Close()
}
