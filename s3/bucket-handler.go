package s3

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	svcS3 "github.com/aws/aws-sdk-go/service/s3"
	PrintlnC "github.com/fatih/color"
)

// MakeBucket makes a new bucket
func (c *S3Client) MakeBucket() error {
	if _, err := c.s3Client.CreateBucket(&svcS3.CreateBucketInput{Bucket: c.BucketName}); err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case svcS3.ErrCodeBucketAlreadyExists:
				PrintlnC.Magenta("The bucket %s already exists. Error: %s\n", *c.BucketName, awsErr.Error())
				return nil
			case svcS3.ErrCodeNoSuchKey:
				PrintlnC.Magenta("The bucket %s Already Owned By You. Error: %s\n", *c.BucketName, awsErr.Error())
				return nil
			default:
				PrintlnC.Red(awsErr.Error())
			}
		} else {
			PrintlnC.Red(err.Error())
		}
		return err
	}
	PrintlnC.Green("The bucket %s was created successfully\n", *c.BucketName)
	return nil
}

// DeleteBucket deletes a bucket
func (c *S3Client) DeleteBucket() {
	if _, err := c.s3Client.DeleteBucket(&svcS3.DeleteBucketInput{Bucket: c.BucketName}); err != nil {
		PrintError(err)
		return
	}
	PrintlnC.Magenta("Waiting for bucket %s to be deleted...\n", *c.BucketName)
	if err := c.s3Client.WaitUntilBucketNotExists(&svcS3.HeadBucketInput{Bucket: c.BucketName}); err != nil {
		PrintError(err)
		return
	}
	PrintlnC.Green("The bucket %s was successfully deleted\n", *c.BucketName)
}

// HeadBucket determines if a bucket exists, and you have permission to access it
func (c *S3Client) HeadBucket() {
	result, err := c.s3Client.HeadBucket(nil)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case svcS3.ErrCodeNoSuchBucket:
				PrintlnC.Red("The bucket %s already exists. Error: %s\n", *c.BucketName, awsErr.Error())
			default:
				PrintlnC.Red(awsErr.Error())
			}
		} else {
			PrintlnC.Red(err.Error())
		}
		return
	}
	PrintlnC.Green("HeadBucket succeeded - %s\n", result.String())
}

// ListBuckets prints the list of the existing buckets
func (c *S3Client) ListBuckets() {
	result, err := c.s3Client.ListBuckets(nil)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case svcS3.ErrCodeNoSuchBucket:
				PrintlnC.Magenta("The bucket %s already exists. Error: %s\n", *c.BucketName, awsErr.Error())
			default:
				PrintlnC.Magenta(awsErr.Error())
			}
		} else {
			PrintlnC.Red(err.Error())
		}
		return
	}
	PrintlnC.Cyan("List Buckets:")
	for _, bucket := range result.Buckets {
		PrintlnC.Cyan("* %s\n", aws.StringValue(bucket.Name))
	}
}
