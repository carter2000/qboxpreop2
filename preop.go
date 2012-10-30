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
	qconfig "qbox.us/cc/config"
	"qbox.us/log"
	"qbox.us/rpc"
)

var (
	confName = flag.String("c", "preop.conf", "config file")
)

const (
	logPrefix = "data_"
)

type config struct {
	IODomain	string	`json:"io_domain"`
	LogIndex	int	`json:"log_index"`
	Params		[]string`json:"params"`
	Off		int64	`json:"off"`
	IntervalNS	int64	`json:"interval_ns"`
	MaxLogSize	int64	`json:"max_logsize`
	Uids		[]uint32`json:"uids"`
	Follow		bool	`json:"follow"`
	Save		bool	`json:"save"`
	DebugLevel	int	`json:"debug_level"`
	RW		sync.RWMutex	`json:"-"`
}

func main() {

	flag.Parse()

	var conf config
	if err := qconfig.LoadEx(&conf, *confName); err != nil {
		return
	}

	log.SetOutputLevel(conf.DebugLevel)

	logName := logPrefix + strconv.Itoa(conf.LogIndex)
	logFile, err := os.Open(logName)
	if err != nil {
		log.Fatal("open ", logName, " fail: ", err)
	}
	defer func() {
		logFile.Close()
	}()

	defer save(&conf)
	go interruptSave(&conf)
	go successiveSave(&conf)

	var reader *bufio.Reader
	reader, conf.Off = getReader(logFile, conf.Off)

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

			newFile := reOpen(logFile, conf.MaxLogSize)
			if newFile == nil {
				continue
			}

			if logFile.Name() != newFile.Name() {
				conf.RW.Lock()
				conf.Off = 0
				conf.LogIndex = getLogIndex(newFile.Name())
				conf.RW.Unlock()
				log.Debug("reOpen ", newFile.Name())
			}
			logFile.Close()
			logFile = newFile

			reader, conf.Off = getReader(logFile, conf.Off)

			continue
		}

		atomic.AddInt64(&conf.Off, int64(len(line)) + 1)

		if uri, ok := parseLine(string(line), conf.Uids); ok {
			preOp(conf.IODomain, string(uri), conf.Params, conf.Save)
		}

		line = line[:0]
	}
}

func parseLine(line string, uids []uint32) (uri string, ok bool) {

	const (
		RS_REQ = "REQ\tRS\t"
		RS_PUT = "/put/"
		CONTENT_TYPE = "\"Content-Type\":"
		TOKEN_UID = "\"uid\":"
	)

	if !strings.HasPrefix(line, RS_REQ) {
		return
	}

	words := strings.SplitN(line[len(RS_REQ):], "\t", 6)
	if len(words) < 6 {
		return
	}

	if words[1] != "POST" || words[4] != "200" {
		return
	}

	url := words[2]
	if !strings.HasPrefix(url, RS_PUT) {
		return
	}

	entry := url[len(RS_PUT):]
	if i := strings.Index(entry, "/"); i >= 0 {
		entry = entry[:i]
	}
	log.Debug("parse entry: ", entry)

	header := words[3]
	ctIdx := strings.Index(header, CONTENT_TYPE)
	if ctIdx < 0 {
		return
	}

	if !strings.HasPrefix(header[ctIdx + len(CONTENT_TYPE) + 1:], "\"image/") {
		return
	}

	uidIdx := strings.Index(header, TOKEN_UID)
	if uidIdx < 0 {
		return
	}

	strUid := header[uidIdx + len(TOKEN_UID) + 1:]
	if i := strings.Index(strUid, ","); i < 0 {
		strUid = strUid[:i]
	}

	uid, err := strconv.ParseUint(strUid, 10, 32)
	if err != nil {
		return
	}

	log.Debug("parse uid: ", uid)
	if !contain(uids, uint32(uid)) {
		return
	}

	entryURI, err := rpc.DecodeURI(entry)
	if err != nil {
		log.Warn(entryURI, " decode fail: ", err)
		return
	}

	uri = strings.Replace(entryURI, ":", "/", 1)
	return uri, true
}

func contain(a []uint32, t uint32) bool {

	for _, v := range a {
		if v == t {
			return true
		}
	}
	return false
}

func getReader(f *os.File, off int64) (*bufio.Reader, int64) {

	newOff, err := f.Seek(off, os.SEEK_SET)
	if err != nil {
		log.Warn("Seek fail: ", err)
	}
	return bufio.NewReader(f), newOff
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

	var closestF *os.File
	closestIdx := -1
	minIdx := getLogIndex(name)
	for _, fn := range fns {
		idx := getLogIndex(fn)
		if idx <= minIdx {
			continue
		}

		f, err := os.Open(fn)
		if err != nil {
			continue
		}

		if closestF != nil && closestIdx < idx {
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

func reOpen(f *os.File, maxSize int64) (nf *os.File) {

	oldFi, err := f.Stat()
	if err != nil {
		log.Warn("stat", err)
		return
	}

	if oldFi.Size() >= maxSize {
		return getNextLog(f.Name())
	}

	newFi, err := os.Stat(f.Name())
	if err != nil {
		log.Warn("stat", err)
		return
	}

	if newFi.Size() == oldFi.Size() {
		return
	}

	f2, err := os.Open(f.Name())
	if err != nil {
		log.Warn("open", f.Name(), "fail:", err)
		return
	}

	return f2
}

func save(conf *config) {

	conf.RW.RLock()
	b, err := json.MarshalIndent(conf, "", "\t")
	conf.RW.RUnlock()
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

func interruptSave(conf *config) {

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	for _ = range c {
		save(conf)
		os.Exit(0)
	}
}

func successiveSave(conf *config) {

	c := time.Tick(time.Minute)
	for _ = range c {
		save(conf)
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
