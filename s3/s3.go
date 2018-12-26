package s3

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"os"
	"path/filepath"

	aws "github.com/aws/aws-sdk-go/aws"
	credentials "github.com/aws/aws-sdk-go/aws/credentials"
	session "github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	s3 "github.com/aws/aws-sdk-go/service/s3"
)

// Init - Init
func Init(accessKeyID, secretAccessKey, bucketRegion string) (*awss3.S3, error) {
	creds := credentials.NewStaticCredentials(accessKeyID, secretAccessKey, "")
	_, err := creds.Get()
	if err != nil {
		return nil, fmt.Errorf("Bad credentials: %v", err)
	}

	cfg := aws.NewConfig().WithRegion(bucketRegion).WithCredentials(creds)
	svc := s3.New(session.New(), cfg)

	return svc, nil
}

// Upload - Upload
func Upload(bucketName string, svc *awss3.S3, file multipart.File, header multipart.FileHeader, docType, filename string) error {

	path := "/" + docType + "/" + filename

	fileBytes, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}

	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(path),
		ACL:                  aws.String("private"),
		Body:                 bytes.NewReader(fileBytes),
		ContentLength:        aws.Int64(header.Size),
		ContentType:          aws.String(header.Header.Get("Content-Type")),
		ContentDisposition:   aws.String("attachment"),
		ServerSideEncryption: aws.String("AES256"),
	})

	return err
}

// Download - Download
func Download(bucketName string, svc *awss3.S3, docType, filename string) (string, error) {

	path := "/" + docType + "/" + filename
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(path),
	}

	obj, err := svc.GetObject(input)
	if err != nil {
		return "", err
	}

	lpath := filepath.Join(".", "images")
	if _, err := os.Stat(lpath); os.IsNotExist(err) {
		if err = os.Mkdir(lpath, os.ModePerm); err != nil {
			return "", err
		}
	}

	imagePath := lpath + "/" + filename
	outFile, err := os.Create(imagePath)
	if err != nil {
		return "", err
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, obj.Body)
	if err != nil {
		return "", err
	}
	return imagePath, nil
}

// DownloadImage - DownloadImage
func DownloadImage(bucketName string, svc *awss3.S3, docType, filename string) (*s3.GetObjectOutput, error) {

	path := "/" + docType + "/" + filename
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(path),
	}

	obj, err := svc.GetObject(input)
	if err != nil {
		return obj, err
	}
	return obj, nil
}
