package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	optarg "github.com/jteeuwen/go-pkg-optarg"
	"github.com/juju/ratelimit"
)

var (
	accessKey   string
	secretKey   string
	bucket      string
	region      string
	profileName string
	cred        *credentials.Credentials
	maxRate     string
	reverse     bool
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
	file, err := os.Open(src)
	if err != nil {
		return err
	}
	config := aws.Config{
		Credentials: cred,
		Region:      aws.String(region),
	}

	limiter := ratelimit.NewBucketWithRate(parseRateLimit(maxRate), 100*1024)
	uploader := s3manager.NewUploader(session.New(&config))
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
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
	if len(profileName) != 0 {
		result = credentials.NewSharedCredentials("", profileName)
		if result == nil {
			return nil, errors.New("no credentials")
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
	optarg.Header("s3 Options")
	optarg.Add(
		"",
		"access_key",
		"AWSのアクセスキー. 未設定のときは環境変数の値が使用される.",
		getEnv("AWS_ACCESS_KEY", ""),
	)
	optarg.Add(
		"",
		"secret_key",
		"AWSのシークレットキー. 未設定のときは環境変数の値が使用される.",
		getEnv("AWS_SECRET_KEY", ""),
	)
	optarg.Add(
		"b",
		"bucket",
		"バケット名",
		getEnv("S3_BUCKET_NAME", ""),
	)
	optarg.Add(
		"",
		"region",
		"s3のリージョン名",
		getEnv("S3_REGION_NAME", "ap-northeast-1"),
	)
	optarg.Add(
		"p",
		"profile",
		"使用するプロファイル",
		"",
	)

	optarg.Header("tx Options")
	optarg.Add(
		"m",
		"max_rate",
		"最大転送速度",
		"",
	)
	optarg.Add(
		"r",
		"reverse",
		"再帰モード",
		false,
	)
	for opt := range optarg.Parse() {
		switch opt.Name {
		case "access_key":
			accessKey = opt.String()
		case "secret_key":
			secretKey = opt.String()
		case "bucket":
			bucket = opt.String()
		case "region":
			region = opt.String()
		case "profile":
			profileName = opt.String()
		case "max_rate":
			maxRate = opt.String()
		case "reverse":
			reverse = opt.Bool()
		}
	}
	var (
		src string
		dst string
		err error
	)

	if len(optarg.Remainder) != 2 {
		log.Fatalln(errors.New("引数が不正です"))
	}
	src = optarg.Remainder[0]
	dst = optarg.Remainder[1]
	cred, err = getCredential()
	if err != nil {
		log.Fatalln(err)
	}

	if reverse {
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

	//uploadReverse(".", "")
}
