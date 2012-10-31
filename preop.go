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
	ImageSuffixs	[]string`json:"image_suffixs"`
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

		// assume isPrefix nerver be true
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
				log.Info("reOpen ", newFile.Name())
			}
			logFile.Close()
			logFile = newFile

			reader, conf.Off = getReader(logFile, conf.Off)

			continue
		}

		atomic.AddInt64(&conf.Off, int64(len(line)) + 1)

		if uri, ok := parseLine(string(line), conf.Uids, conf.ImageSuffixs); ok {
			preOp(conf.IODomain, string(uri), conf.Params, conf.Save)
		}

		line = line[:0]
	}
}

func parseLine(line string, uids []uint32, imgSuffixs []string) (uri string, ok bool) {

	const (
		RS_REQ = "REQ\tRS\t"
		RS_PUT = "/put/"
		RS_INS = "/ins/"
		MIME_TYPE = "mimeType/"
		TOKEN_UID = "\"uid\":"
	)

	if !strings.HasPrefix(line, RS_REQ) {
		return
	}

	words := strings.SplitN(line[len(RS_REQ):], "\t", 7)
	if len(words) < 7 {
		return
	}

	if words[1] != "POST" || words[5] != "200" {
		return
	}

	url := words[2]
	if !strings.HasPrefix(url, RS_PUT) && !strings.HasPrefix(url, RS_INS) {
		return
	}

	// len(RS_PUT) == len(RS_INS)
	param := ""
	entry := url[len(RS_PUT):]
	if i := strings.Index(entry, "/"); i >= 0 {
		param = entry[i:]
		entry = entry[:i]
	}

	entry, err := rpc.DecodeURI(entry)
	if err != nil {
		log.Warn(entry, " decode fail: ", err)
		return
	}

	mime := ""
	if i := strings.Index(param, MIME_TYPE); i >= 0 {
		mime = param[i+len(MIME_TYPE):]
		if j := strings.Index(mime, "/"); j >= 0 {
			mime = mime[:j]
		}
	}

	mime, err = rpc.DecodeURI(mime)
	if err != nil {
		log.Warn(mime, "decode fail: ", err)
		return
	}

	if !strings.HasPrefix(mime, "image/") && !hasSuffix(entry, imgSuffixs) {
		return
	}

	header := words[3]
	uidIdx := strings.Index(header, TOKEN_UID)
	if uidIdx < 0 {
		return
	}

	strUid := header[uidIdx + len(TOKEN_UID):]
	if i := strings.Index(strUid, ","); i >= 0 {
		strUid = strUid[:i]
	}

	uid, err := strconv.ParseUint(strUid, 10, 32)
	if err != nil {
		return
	}

	if !contain(uids, uint32(uid)) {
		return
	}

	uri = strings.Replace(entry, ":", "/", 1)
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

func hasSuffix(s string, suffixs []string) bool {

	for _, v := range suffixs {
		if strings.HasSuffix(s, v) {
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

var successiveFail = 0

func reOpen(f *os.File, maxSize int64) (nf *os.File) {

	oldFi, err := f.Stat()
	if err != nil {
		log.Warn("stat", err)
		return
	}

	successiveFail++
	if successiveFail > 100 || oldFi.Size() >= maxSize {
		successiveFail = 0
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
