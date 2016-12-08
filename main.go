package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var (
	outputDir           = "output"
	fileExtension       = ".txt"
	outputFileExtension = ".7z"
)

var MAX_FILES = 1024
var fileSemaphore = make(chan bool, MAX_FILES)

func main() {
	files, err := filepath.Glob("data/*" + fileExtension)
	if err != nil {
		panic(err)
	}

	// ensure the output directory exists
	err = os.MkdirAll(filepath.Join(".", outputDir), 0777)
	if err != nil {
		panic(err)
	}

	// spin up a bunch of goroutines, each one handling a particular permutation of the input files.
	// these goroutines read in the input files and write an output file to the `outputDir`.

	for i := 0; i < MAX_FILES; i++ {
		fileSemaphore <- true
	}

	chchDone := make(chan chan struct{})

	go func() {
		i := 0
		for x := range permutations(files) {
			chDone := make(chan struct{})
			outfileName := fmt.Sprintf("out-%v%v", i, outputFileExtension)
			go concatFiles(x, fileExtension, outputDir, outfileName, chDone)
			chchDone <- chDone
			i++
		}
	}()

	// wait for our concurrent file operations to finish
	for chDone := range chchDone {
		<-chDone
	}

	fmt.Println("Done.")
}

func permutations(elems []string) chan []string {
	chOut := make(chan []string)

	go func() {
		if len(elems) <= 1 {
			chOut <- elems
			close(chOut)
			return
		}

		for perm := range permutations(elems[1:]) {
			for i := range elems {
				tmp := []string{}
				tmp = append(tmp, perm[:i]...)
				tmp = append(tmp, elems[0])
				tmp = append(tmp, perm[i:]...)

				chOut <- tmp
			}
		}
		close(chOut)
	}()

	return chOut
}

func closeFile(f *os.File) {
	if err := f.Close(); err != nil {
		panic(err)
	}
	fileSemaphore <- true
}

func concatFiles(files []string, ext string, outputDir string, outfileName string, chDone chan struct{}) {
	destPath := filepath.Join(outputDir, outfileName)

	// create the output file
	<-fileSemaphore
	destFile, err := os.Create(destPath)
	if err != nil {
		panic(err)
	}
	defer closeFile(destFile)

	for _, filename := range files {
		func(filename string) {
			<-fileSemaphore
			srcFile, err := os.Open(filename)
			if err != nil {
				panic(err)
			}
			defer closeFile(srcFile)

			buf := make([]byte, 1024*1024)
			for {
				n, err := srcFile.Read(buf)
				if err == io.EOF {
					break
				} else if err != nil {
					panic(err)
				}

				tmpbuf := make([]byte, n)
				copy(tmpbuf, buf)

				_, err = destFile.Write(tmpbuf)
				if err != nil {
					panic(err)
				}
			}
		}(filename)
	}

	fmt.Printf("Finished writing %v\n", destPath)

	chDone <- struct{}{}
}
