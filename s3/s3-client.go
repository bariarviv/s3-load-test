package s3

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	svcS3 "github.com/aws/aws-sdk-go/service/s3"
	PrintlnC "github.com/fatih/color"
)

type Config struct {
	AccessKey  string `json:"access_key"`
	SecretKey  string `json:"secret_key"`
	Region     string `json:"region_name"`
	Endpoint   string `json:"endpoint_url"`
	BucketName string `json:"bucket_name"`
}

type S3Client struct {
	Session    *session.Session
	s3Client   *svcS3.S3
	BucketName *string
}

// ConfigCredentials initializes the credentials and creates an S3 service client
func (c *S3Client) ConfigCredentials(config *Config) error {
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, ""),
		Endpoint:         aws.String(config.Endpoint),
		Region:           aws.String(config.Region),
		S3ForcePathStyle: aws.Bool(true),
	}
	// Creates S3 service client
	c.Session = session.Must(session.NewSession(s3Config))
	c.BucketName = aws.String(config.BucketName)
	c.s3Client = svcS3.New(c.Session)
	return nil
}

func SetConfig(accessKey, secretKey, region, endpoint, bucketName string) *Config {
	return &Config{
		AccessKey:  accessKey,
		SecretKey:  secretKey,
		Region:     region,
		Endpoint:   endpoint,
		BucketName: bucketName,
	}
}

func PrintError(err error) {
	if awsErr, ok := err.(awserr.Error); ok {
		switch awsErr.Code() {
		default:
			PrintlnC.Red(awsErr.Error())
		}
	}
	PrintlnC.Red(err.Error())
}
