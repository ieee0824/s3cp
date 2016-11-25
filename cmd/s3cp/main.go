package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/juju/ratelimit"
)

var (
	accessKey = flag.String("access_key", getEnv("AWS_ACCESS_KEY", ""), `
-access_key (string)
	Overrides env settings.
	`)
	secretKey = flag.String("secret_key", getEnv("AWS_SECRET_KEY", ""), `
-secret_key (string)
	Overrides env settings.
	`)
	bucket = flag.String("b", getEnv("S3_BUCKET_NAME", ""), `
-b (string)
	The bucket to use. Overrides env settings.
	`)
	region = flag.String("region", getEnv("S3_REGION_NAME", "ap-northeast-1"), `
-region (string)
	The region to use. Overrides env settings.
	`)
	profileName = flag.String("p", "", `
-p (string)
	Use a specific profile from your credential file.
	`)
	cred    *credentials.Credentials
	maxRate = flag.String("max", "", `
-max (string)
	Transfer rate.
	-max 100k
	-max 1M
	`)
	reverse = flag.Bool("r", false, "Reverse the order of the sort to get reverse lexicographical order or the oldest entries first (or largest files last, if combined with sort by size")
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
		Region:      aws.String(*region),
	}

	limiter := ratelimit.NewBucketWithRate(parseRateLimit(*maxRate), 100*1024)
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
	if len(*profileName) != 0 {
		result = credentials.NewSharedCredentials("", *profileName)
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

func main() {
	flag.Parse()
}
