package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

type warcRecord struct {
	header string
	body   []byte
}

type pair struct {
	text string
	lang string
}

func walkFiles(done <-chan struct{}, root string) (<-chan string, <-chan error) {
	paths := make(chan string)
	errc := make(chan error, 1)
	go func() { // HL
		// Close the paths channel after Walk returns.
		defer close(paths) // HL
		// No select needed for this send, since errc is buffered.
		errc <- filepath.Walk(root, func(path string, info os.FileInfo, err error) error { // HL
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			select {
			case paths <- path: // HL
			case <-done: // HL
				return errors.New("walk canceled")
			}
			return nil
		})
	}()
	return paths, errc
}

func main() {
	done := make(chan struct{})
	defer close(done)

	paths, errc := walkFiles(done, "data/")
	file, err := os.Open("data/wetfile_1.wet")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	waiter := make(chan struct{})
	lines := make(chan pair, 40)

	go clasifyFiles(waiter, lines)

	var wg sync.WaitGroup
	maxGoroutines := 40
	guard := make(chan struct{}, maxGoroutines)
	for path := range paths {
		wg.Add(1)
		go func(path string) {
			guard <- struct{}{}
			clasifyLines(path, lines) // HLc
			<-guard
			wg.Done()
		}(path)
	}

	// Check whether the Walk failed.
	if err := <-errc; err != nil { // HLerrc
		log.Fatal(err)
	}
	wg.Wait()
	close(lines)
	<-waiter
}

//readWarcRecord reads one warc record from Reader
//  warc-record  = header CRLF
//  block CRLF CRLF
func readWarcRecord(in *bufio.Reader) (warcRecord, error) {

	var ret warcRecord

	line, err := in.ReadBytes('\n')
	if err != nil {
		return ret, err
	}

	firstLine := string(line)

	//Warc record starts with version e.g. "WARC/1.0"
	if firstLine != "WARC/1.0\r\n" {
		return ret, fmt.Errorf("warc version expected '%s' found", firstLine)
	}
	var warcHeaderBuilder strings.Builder

	var contentLength = -1

	//read header till end (\n)
	for ; string(line) != "\r\n"; line, err = in.ReadBytes('\n') {

		if err != nil {
			return ret, err
		}

		//each header must contains Content-Length
		//alse named headers are case insensitive
		if strings.HasPrefix(strings.ToLower(string(line)), "content-length:") {

			if contentLength > 0 {
				return ret, fmt.Errorf("exactly one content-length should be present in a WARC header")
			}

			keyAndValue := strings.SplitN(string(line), ":", 2)
			if len(keyAndValue) != 2 {
				return ret, fmt.Errorf("Content-Length field must contains a value. '%s' found)", line)
			}
			//field value may be preceded by any  amount  of  linear  whitespace
			strValue := strings.TrimSpace(keyAndValue[1])
			contentLength, err = strconv.Atoi(strValue)
			if err != nil {
				return ret, err
			}
		}

		warcHeaderBuilder.Write(line)
	}

	//content length sould be non-negative
	if contentLength < 0 {
		return ret, fmt.Errorf("exactly one content-length should be present in a WARC header. WARC header: %s", warcHeaderBuilder.String())
	}

	//early return if body is empty
	if contentLength == 0 {
		return warcRecord{warcHeaderBuilder.String(), []byte{}}, nil
	}

	//body buffer
	body := make([]byte, contentLength)

	n := 0
	//put reader date to body buffer
	for k, err := in.Read(body); n < contentLength; k, err = in.Read(body[n:]) {
		if err != nil && err != io.EOF {
			return ret, err
		}
		if err == io.EOF && (n+k) < contentLength {
			return ret, fmt.Errorf("WARC record finished unexpectedly. Content-Length : %d, got %d", contentLength, n)
		}
		n += k
	}

	return warcRecord{warcHeaderBuilder.String(), body}, err
}

func clasifyLines(path string, lines chan<- pair) {
	in, err := os.Open(path)
	if err != nil {
		fmt.Println(err)
	}
	bufin := bufio.NewReader(in)

	_, fileName := filepath.Split(path)

	var tagFile strings.Builder
	tagFile.WriteString("tmp/")
	tagFile.WriteString(fileName)
	tagFile.WriteString("_tag.txt")

	tags, err := os.Create(tagFile.String())
	if err != nil {
		log.Fatal(err)
	}

	var cleanFile strings.Builder
	cleanFile.WriteString("tmp/")
	cleanFile.WriteString(fileName)
	cleanFile.WriteString("_clean.txt")

	clean, err := os.Create(cleanFile.String())
	if err != nil {
		log.Fatal(err)
	}

	cmd := exec.Command("fastText/fasttext", "predict-prob", "fastText/lid.176.bin", "-")

	var label = regexp.MustCompile(`\_\_label\_\_([a-z]+)\s([0-9]*\.?[0-9]*)`)

	stdin, err := cmd.StdinPipe()
	cmd.Stdout = tags

	go func() {
		defer stdin.Close()
		for record, err := readWarcRecord(bufin); err == nil; record, err = readWarcRecord(bufin) {
			buf := bytes.NewBuffer(record.body)
			for line, err := buf.ReadString('\n'); err == nil; line, err = buf.ReadString('\n') {
				if utf8.RuneCountInString(line) > 100 && utf8.Valid([]byte(line)) {
					io.WriteString(stdin, line)
					fmt.Fprint(clean, line)
				}
			}
			bufin.ReadBytes('\n')
			bufin.ReadBytes('\n')
		}
	}()

	err = cmd.Run()
	if err != nil {
		log.Fatal(err)
	}

	if err := tags.Close(); err != nil {
		log.Fatal(err)
	}
	if err := clean.Close(); err != nil {
		log.Fatal(err)
	}

	tags, err = os.Open(tagFile.String())
	if err != nil {
		log.Fatal(err)
	}
	tagread := bufio.NewReader(tags)

	clean, err = os.Open(cleanFile.String())
	if err != nil {
		log.Fatal(err)
	}
	cleanread := bufio.NewReader(clean)

	for par, err := cleanread.ReadString('\n'); err == nil; par, err = cleanread.ReadString('\n') {
		tag, err := tagread.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}
		var prob float64
		tagParts := label.FindStringSubmatch(tag)
		lang := tagParts[1]
		fmt.Sscan(tagParts[2], &prob)
		if prob <= 0.8 {
			continue
		}
		lines <- pair{par, lang}
	}
	if err := tags.Close(); err != nil {
		log.Fatal(err)
	}
	if err := clean.Close(); err != nil {
		log.Fatal(err)
	}

	if err := os.Remove(cleanFile.String()); err != nil {
		log.Fatal(err)
	}
	if err := os.Remove(tagFile.String()); err != nil {
		log.Fatal(err)
	}
}

func clasifyFiles(waiter chan struct{}, lines <-chan pair) {
	m := make(map[string]*os.File)
	var err error
	for line := range lines {
		if _, ok := m[line.lang]; !ok {
			var b strings.Builder
			b.WriteString("classified/")
			b.WriteString(line.lang)
			b.WriteString(".txt")
			m[line.lang], err = os.OpenFile(b.String(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatal(err)
			}
			if _, err := m[line.lang].WriteString(line.text); err != nil {
				log.Fatal(err)
			}
		} else {
			if _, err := m[line.lang].WriteString(line.text); err != nil {
				log.Fatal(err)
			}
		}
	}

	for lang := range m {
		if err := m[lang].Close(); err != nil {
			log.Fatal(err)
		}
	}
	waiter <- struct{}{}
}
