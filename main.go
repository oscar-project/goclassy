package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
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

const langsFastText = "af als am an ar arz as ast av az azb ba bar bcl be bg bh bn bo bpy br bs bxr ca cbk ce ceb ckb co cs cv cy da de diq dsb dty dv el eml en eo es et eu fa fi fr frr fy ga gd gl gn gom gu gv he hi hif hr hsb ht hu hy ia id ie ilo io is it ja jbo jv ka kk km kn ko krc ku kv kw ky la lb lez li lmo lo lrc lt lv mai mg mhr min mk ml mn mr mrj ms mt mwl my myv mzn nah nap nds ne new nl nn no oc or os pa pam pfl pl pms pnb ps pt qu rm ro ru rue sa sah sc scn sco sd sh si sk sl so sq sr su sv sw ta te tg th tk tl tr tt tyv ug uk ur uz vec vep vi vls vo wa war wuu xal xmf yi yo yue zh"

type warcRecord struct {
	header string
	body   []byte
}

type pair struct {
	text string
	tags string
}

type langFile struct {
	mux    sync.Mutex
	Writer *bufio.Writer
}

func (lf *langFile) WriteString(s string) (n int, err error) {
	lf.mux.Lock()
	defer lf.mux.Unlock()
	return lf.Writer.WriteString(s)
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

	paths, errc := walkFiles(done, os.Args[1])

	waiter := make(chan struct{})
	files := make(chan pair, 100)
	data := make(chan string, 50)

	go clasifyFiles(waiter, files)
	go clasifyLines(data, files)

	var wg sync.WaitGroup
	maxGoroutines := 20
	guard := make(chan struct{}, maxGoroutines)
	for path := range paths {
		wg.Add(1)
		go func(path string) {
			guard <- struct{}{}
			err := extract(path, data) // HLc
			if err != nil {
				log.Fatalln(err)
			}
			<-guard
			wg.Done()
		}(path)
	}

	// Check whether the Walk failed.
	if err := <-errc; err != nil { // HLerrc
		log.Fatal(err)
	}
	wg.Wait()
	close(data)
	<-waiter
}

// Taken and adapted from https://github.com/ChrisCates/CommonCrawler
// MIT License, Copyright (c) 2017 Chris Cates
func extract(path string, data chan<- string) error {
	//get extracted file path
	fname := filepath.Base(path)
	ext := filepath.Ext(fname)
	fileName := fname[:len(fname)-len(ext)]
	//create extruction destination

	var extractedPath strings.Builder
	extractedPath.WriteString("data/")
	extractedPath.WriteString(fileName)

	out, err := os.Create(extractedPath.String())
	if err != nil {

		return err
	}
	defer out.Close()

	//open gzip file
	fi, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fi.Close()
	//create gz reader
	fz, err := gzip.NewReader(fi)
	if err != nil {
		return err
	}
	defer fz.Close()

	//write extracted to file
	_, err = io.Copy(out, fz)
	if err != nil {
		return err
	}

	data <- extractedPath.String()

	return nil
}

// Taken from https://github.com/ChrisCates/CommonCrawler
// MIT License, Copyright (c) 2017 Chris Cates
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

func clasifyLines(data <-chan string, files chan<- pair) {
	var wg sync.WaitGroup
	maxGoroutines := 10
	guard := make(chan struct{}, maxGoroutines)
	for path := range data {
		wg.Add(1)
		go func(path string) {
			guard <- struct{}{}

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
			cleanWriter := bufio.NewWriter(clean)

			cmd := exec.Command("fastText/fasttext", "predict-prob", "fastText/lid.176.bin", "-")

			stdin, err := cmd.StdinPipe()
			cmd.Stdout = tags

			go func() {
				defer stdin.Close()
				for record, err := readWarcRecord(bufin); err == nil; record, err = readWarcRecord(bufin) {
					buf := bytes.NewBuffer(record.body)
					for line, err := buf.ReadString('\n'); err == nil; line, err = buf.ReadString('\n') {
						if utf8.RuneCountInString(line) > 100 && utf8.Valid([]byte(line)) {
							io.WriteString(stdin, line)
							cleanWriter.WriteString(line)
						}
					}
					bufin.ReadBytes('\n')
					bufin.ReadBytes('\n')
				}
				cleanWriter.Flush()
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

			files <- pair{cleanFile.String(), tagFile.String()}
			in.Close()
			if err := os.Remove(path); err != nil {
				log.Fatal(err)
			}
			<-guard
			wg.Done()
		}(path)
	}
	wg.Wait()
	close(files)
}

func clasifyFiles(waiter chan struct{}, files <-chan pair) {
	m := make(map[string]*langFile)
	scanner := bufio.NewScanner(strings.NewReader(langsFastText))
	scanner.Split(bufio.ScanWords)
	for scanner.Scan() {
		var b strings.Builder
		b.WriteString("classified/")
		b.WriteString(scanner.Text())
		b.WriteString(".txt")
		aux, err := os.OpenFile(b.String(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		defer aux.Close()
		if err != nil {
			log.Fatal(err)
		}
		m[scanner.Text()] = &langFile{Writer: bufio.NewWriter(aux)}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading input:", err)
	}
	var label = regexp.MustCompile(`\_\_label\_\_([a-z]+)\s([0-9]*\.?[0-9]*)`)
	var wg sync.WaitGroup
	//var waitFile sync.WaitGroup
	maxGoroutines := 5
	guard := make(chan struct{}, maxGoroutines)
	for file := range files {
		wg.Add(1)
		go func(file pair) {
			guard <- struct{}{}
			tags, err := os.Open(file.tags)
			if err != nil {
				log.Fatal(err)
			}
			tagread := bufio.NewReader(tags)

			clean, err := os.Open(file.text)
			if err != nil {
				log.Fatal(err)
			}
			cleanread := bufio.NewReader(clean)
			prev := "0"
			var b strings.Builder
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
				if lang == prev || prev == "0" {
					b.WriteString(par)
					prev = lang
				} else {
					b.WriteString("\n")
					if _, err := m[prev].WriteString(b.String()); err != nil {
						log.Fatal(err)
					}
					b.Reset()
					prev = lang
					b.WriteString(par)
				}
			}

			if _, err := m[prev].WriteString(b.String()); err != nil {
				log.Fatal(err)
			}
			b.Reset()

			if err := tags.Close(); err != nil {
				log.Fatal(err)
			}
			if err := clean.Close(); err != nil {
				log.Fatal(err)
			}

			if err := os.Remove(file.text); err != nil {
				log.Fatal(err)
			}
			if err := os.Remove(file.tags); err != nil {
				log.Fatal(err)
			}
			<-guard
			wg.Done()
		}(file)
	}
	wg.Wait()
	//waitFile.Wait()

	for lang := range m {
		if err := m[lang].Writer.Flush(); err != nil {
			log.Fatal(err)
		}
	}
	waiter <- struct{}{}
}
