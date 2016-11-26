package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"flag"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/juju/ratelimit"
)

var (
	accessKey = flag.String(
		"access_key",
		getEnv("AWS_ACCESS_KEY", ""),
		"デフォルトは環境変数",
	)
	secretKey = flag.String(
		"secret_key",
		getEnv("AWS_SECRET_KEY", ""),
		"デフォルトは環境変数",
	)
	bucket = flag.String(
		"b",
		getEnv("S3_BUCKET_NAME", ""),
		"デフォルトは環境変数",
	)
	region = flag.String(
		"region",
		getEnv("S3_REGION_NAME", "ap-northeast-1"),
		"s3リージョン",
	)

	profileName = flag.String(
		"profile",
		"default",
		"awsのクレデンシャル",
	)
	cred    *credentials.Credentials
	maxRate = flag.String(
		"m",
		"",
		"-m 1024k, -m 1024m",
	)
	reverse = flag.Bool(
		"r",
		false,
		"再帰的にファイルをコピーする",
	)
)

func parseRateLimit(r string) float64 {
	if r == "" {
		return 1 * 1024 * 1024 * 1024
	}
	if strings.Contains(r, "k") || strings.Contains(r, "K") {
		r = strings.Replace(r, "k", "", 1)
		r = strings.Replace(r, "K", "", 1)
		i, err := strconv.Atoi(r)
		if err != nil {
			log.Fatalln(err)
		}
		return float64(i) * 1024
	} else if strings.Contains(r, "m") || strings.Contains(r, "M") {
		r = strings.Replace(r, "m", "", 1)
		r = strings.Replace(r, "M", "", 1)
		i, err := strconv.Atoi(r)
		if err != nil {
			log.Fatalln(err)
		}
		return float64(i) * 1024 * 1024
	} else {
		i, err := strconv.Atoi(r)
		if err != nil {
			log.Fatalln(err)
		}
		return float64(i)
	}
	log.Fatalln(errors.New("illegal format"))
	return 0
}

func upload(src, dst string) error {
	fmt.Println(src, dst)
	file, err := os.Open(src)
	if err != nil {
		return err
	}
	config := aws.Config{
		Credentials: cred,
		Region:      aws.String(*region),
	}
	rate := parseRateLimit(*maxRate)
	fmt.Println(int(rate))

	limiter := ratelimit.NewBucketWithRate(rate, 100*1024)
	uploader := s3manager.NewUploader(session.New(&config))
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(*bucket),
		Key:    aws.String(dst),
		Body:   ratelimit.Reader(file, limiter),
	})

	return err
}

func getEnv(key string, def string) string {
	v := os.Getenv(key)
	if len(v) == 0 {
		return def
	}

	return v
}

func getCredential() (*credentials.Credentials, error) {
	var result *credentials.Credentials
	if len(*accessKey) != 0 && len(*secretKey) != 0 {
		return credentials.NewStaticCredentials(*accessKey, *secretKey, ""), nil
	} else if len(*profileName) != 0 {
		result = credentials.NewSharedCredentials("", *profileName)
		if result == nil {
			return nil, errors.New("no credential")
		}
		return result, nil
	}
	return nil, errors.New("no credentials")
}

func uploadNonReverse(src, dst string) error {
	file, err := os.Open(src)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.IsDir() {
		return errors.New(fmt.Sprintf("src is directory"))
	}
	return upload(src, dst)
}

func uploadReverse(src, dst string) error {
	queue := make(chan string, 1024)
	e := make(chan error)
	dst = strings.TrimSuffix(dst, "/")
	go traverse(src, queue, e)

	for {
		select {
		case path := <-queue:
			err := uploadNonReverse(path, dst+path)
			if err != nil {
				return err
			}
		case err := <-e:
			if err.Error() != "terminate" {
				return err
			}
			return nil
		}
	}
}

func traverse(root string, queue chan<- string, e chan<- error) {
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			queue <- path
		}
		return nil
	})
	if err != nil {
		e <- err
	}
	for {
		if len(queue) == 0 {
			break
		}
	}
	e <- errors.New("terminate")
	close(e)
	close(queue)
}

func main() {
	flag.Parse()
	log.SetFlags(log.Lshortfile)

	var (
		src string
		dst string
		err error
	)

	args := flag.Args()
	if len(args) != 2 {
		log.Fatalln("引数が不正です")
	}
	src = args[0]
	dst = args[1]
	cred, err = getCredential()
	if err != nil {
		log.Fatalln(err)
	}

	if *reverse {
		err := uploadReverse(src, dst)
		if err != nil {
			log.Fatalln(err)
		}
	} else {
		err := uploadNonReverse(src, dst)
		if err != nil {
			log.Fatalln(err)
		}
	}
}
