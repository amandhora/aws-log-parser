package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var (
	Bucket         = "p-jp-logs"                       // Download from this bucket
	Prefix         = "P-JP-EMGR/mod/2016/11/01/"       // Using this key prefix
	LocalDirectory = "s3logs"                          // Into this directory
	extractDir     = "logs"                            // Into this directory
)

func main() {

	/*
		var month = flag.String("m", "", "specify month in digit")
		var day = flag.String("d", "", "specify day in digit")
		flag.Parse()

		fmt.Printf("month = %s\n", *month)
		fmt.Printf("day = %s\n", *day)
	*/

	// S3manager init
	manager := s3manager.NewDownloader(session.New(&aws.Config{Region: aws.String("ap-northeast-1")}))
	d := downloader{bucket: Bucket, dir: LocalDirectory, Downloader: manager}

	// Create S3 session
	client := s3.New(session.New(&aws.Config{Region: aws.String("ap-northeast-1")}))
	params := &s3.ListObjectsInput{Bucket: &Bucket, Prefix: &Prefix}

	/* delete all content from LocalDirectory */
	err := removeContents(LocalDirectory)
	if err != nil {
		panic(err)
	}

	/* Process all objects */
	err = client.ListObjectsPages(params, d.eachPage)
	if err != nil {
		panic(err)
	}

	// start parsing of logs
	logDir := filepath.Join(LocalDirectory, extractDir)

	fmt.Println("Starting Parsing of logs in ", logDir)

	err = parseLogFiles(logDir)
	if err != nil {
		panic(err)
	}

}

type downloader struct {
	*s3manager.Downloader
	bucket, dir string
}

func (d *downloader) eachPage(page *s3.ListObjectsOutput, more bool) bool {
	for _, obj := range page.Contents {
		d.downloadToFile(*obj.Key)
	}

	return true
}

func (d *downloader) downloadToFile(key string) {
	// Create the directories in the path

	// Extract day and month from file pathname
	stringSlice := strings.Split(key, "/")

	stringSlice = stringSlice[2:len(stringSlice)]
	fileName := stringSlice[len(stringSlice)-1]

	newPath := strings.Join(stringSlice, "/")

	// Create filename
	file := filepath.Join(d.dir, newPath)
	if err := os.MkdirAll(filepath.Dir(file), 0775); err != nil {
		panic(err)
	}

	// Set up the local file
	fd, err := os.Create(file)
	if err != nil {
		panic(err)
	}
	defer fd.Close()

	// Download the file using the AWS SDK
	fmt.Printf("Downloading s3://%s/%s to %s...\n", d.bucket, key, file)
	params := &s3.GetObjectInput{Bucket: &d.bucket, Key: &key}

	_, err = d.Downloader.Download(fd, params)
	if err != nil {
		panic(err)
	}

	fileSlice := strings.Split(fileName, ".")

	// TODO unzip to same directory
	err = unGzip(file, fileSlice[0])
	if err != nil {
		panic(err)
	}

}

func unGzip(source, target string) error {
	reader, err := os.Open(source)
	if err != nil {
		return err
	}
	defer reader.Close()

	archive, err := gzip.NewReader(reader)
	if err != nil {
		return err
	}
	defer archive.Close()

	// Create filename
	fileName := filepath.Join(LocalDirectory, extractDir, target)
	if err := os.MkdirAll(filepath.Dir(fileName), 0775); err != nil {
		panic(err)
	}

	writer, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer writer.Close()

	n, err := io.Copy(writer, archive)
	fmt.Println("copied ", n, " bytes to ", fileName)
	return err
}

func removeContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	defer d.Close()

	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}

	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}

type ParseData struct {
	rcvdCnt, redirectCnt       int
	rcvdImpCnt, redirectImpCnt int
	rcvdLpCnt, redirectLpCnt   int
	fileName                   string
}

func parseLogFiles(dir string) error {

	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()

	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}

	c := make(chan ParseData)
	defer close(c)

	goRoutineCnt := 0
	for _, name := range names {
		//err = os.RemoveAll(filepath.Join(dir, name))
		go processFile(c, filepath.Join(dir, name))
		goRoutineCnt++
	}

	for i := 0; i < goRoutineCnt; i++ {
		data := <-c
		fmt.Println("file: ", data.fileName)
		fmt.Println(" [RCVD] Imp: ", data.rcvdImpCnt, " LP: ", data.rcvdLpCnt, "Total: ", data.rcvdCnt)
		fmt.Println(" [REDR] Imp: ", data.redirectImpCnt, " LP: ", data.redirectLpCnt, "Total: ", data.redirectCnt)
	}
	return nil
}

func processFile(ch chan ParseData, fileName string) {

	f, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	data := ParseData{
		rcvdCnt:     0,
		redirectCnt: 0,
		fileName:    fileName}

	// create a new scanner and read the file line by line
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "tppa_pxl_rcvd") {

			// Received TPPA pixels
			data.rcvdCnt++

			if strings.Contains(scanner.Text(), "evt=1") {
				data.rcvdImpCnt++
			} else if strings.Contains(scanner.Text(), "evt=5") {
				data.rcvdLpCnt++
			}
		}
		if strings.Contains(scanner.Text(), "tppa_redirect_pxl") {

			// Redirected TPPA pixels
			data.redirectCnt++

			if strings.Contains(scanner.Text(), "evt=1") {
				data.redirectImpCnt++
			} else if strings.Contains(scanner.Text(), "evt=5") {
				data.redirectLpCnt++
			}
		}
	}

	// check for errors
	if err = scanner.Err(); err != nil {
		panic(err)
	}

	ch <- data
}
