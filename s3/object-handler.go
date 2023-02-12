package s3

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	svcS3 "github.com/aws/aws-sdk-go/service/s3"
	PrintlnC "github.com/fatih/color"
)

// PutObject puts an object to the bucket
func (c *S3Client) PutObject(objectName string) {
	body := "-----------------------------------------test--file-----------------------------------------"
	if _, err := c.s3Client.PutObject(&svcS3.PutObjectInput{Body: aws.ReadSeekCloser(strings.NewReader(body)),
		Bucket: aws.String(*c.BucketName), Key: aws.String(objectName)}); err != nil {
		PrintError(err)
		return
	}
	PrintlnC.Green("The object %s has been uploaded successfully\n", objectName)
}

// DeleteObject deletes an object from the bucket
func (c *S3Client) DeleteObject(objectName string) {
	if _, err := c.s3Client.DeleteObject(&svcS3.DeleteObjectInput{Bucket: c.BucketName, Key: aws.String(objectName)}); err != nil {
		PrintError(err)
		return
	}
	if err := c.s3Client.WaitUntilObjectNotExists(&svcS3.HeadObjectInput{Bucket: c.BucketName, Key: aws.String(objectName)}); err != nil {
		PrintError(err)
		return
	}
	PrintlnC.Green("The object %s was successfully deleted\n", objectName)
}

// HeadObject retrieves metadata from an object without returning the object itself
func (c *S3Client) HeadObject(objectName string) {
	result, err := c.s3Client.HeadObject(&svcS3.HeadObjectInput{Bucket: aws.String(*c.BucketName), Key: aws.String(objectName)})
	if err != nil {
		PrintError(err)
		return
	}
	PrintlnC.Green("HeadObject succeeded - %s\n", result.String())
}

// GetObject gets an object from the bucket
func (c *S3Client) GetObject(objectName string) {
	if _, err := c.s3Client.GetObject(&svcS3.GetObjectInput{Bucket: aws.String(*c.BucketName), Key: aws.String(objectName)}); err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case svcS3.ErrCodeNoSuchKey:
				PrintlnC.Red(svcS3.ErrCodeNoSuchKey, awsErr.Error())
			case svcS3.ErrCodeInvalidObjectState:
				PrintlnC.Red(svcS3.ErrCodeInvalidObjectState, awsErr.Error())
			default:
				PrintlnC.Red(awsErr.Error())
			}
		} else {
			PrintlnC.Red(err.Error())
		}
		return
	}
	PrintlnC.Green("The object %s was received successfully\n", objectName)
}

// ListObjects prints the list of the objects in the bucket
func (c *S3Client) ListObjects() {
	resp, err := c.s3Client.ListObjectsV2(&svcS3.ListObjectsV2Input{Bucket: c.BucketName})
	if err != nil {
		PrintError(err)
		return
	}
	if len(resp.Contents) == 0 {
		PrintlnC.Red("The bucket is empty!")
		return
	}
	PrintlnC.Cyan("List Objects:")
	for _, item := range resp.Contents {
		PrintlnC.Cyan("Object Name: %s, Object Size: %d, Last modified: %s\n", *item.Key, *item.Size, *item.LastModified)
	}
}
