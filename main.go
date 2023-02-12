package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"text/tabwriter"

	PrintlnC "github.com/fatih/color"
	S3 "s3_load_test/s3"
)

var (
	NumOfObjects    int
	Config          S3.Config
	Put, Get, Clean bool
)

const (
	FileName = "file%d"
)

func main() {
	initFlags()
	if err := verifyFlags(); err != nil {
		PrintlnC.Red(err.Error())
		flag.Usage()
		return
	}

	var client S3.S3Client
	if err := client.ConfigCredentials(&Config); err != nil {
		PrintlnC.Red(err.Error())
		return
	}
	if err := client.MakeBucket(); err != nil {
		PrintlnC.Red(err.Error())
		return
	}
	if Put {
		PutLoad(client, NumOfObjects)
	}
	if Get {
		GetLoad(client, NumOfObjects)
	}
	if Clean {
		CleanData(client, NumOfObjects)
	}
}

func GetLoad(client S3.S3Client, numOfObjects int) {
	PutLoad(client, numOfObjects)

	PrintlnC.Cyan("Starting the GET load")
	wg := sync.WaitGroup{}
	wg.Add(numOfObjects)
	for i := 0; i < numOfObjects; i++ {
		go func(syncer *sync.WaitGroup, index int) {
			defer syncer.Done()
			client.GetObject(fmt.Sprintf(FileName, index))
		}(&wg, i)
	}
	wg.Wait()
}

func PutLoad(client S3.S3Client, numOfObjects int) {
	PrintlnC.Cyan("Starting the PUT load")
	wg := sync.WaitGroup{}
	wg.Add(numOfObjects)
	for i := 0; i < numOfObjects; i++ {
		go func(syncer *sync.WaitGroup, index int) {
			defer syncer.Done()
			client.PutObject(fmt.Sprintf(FileName, index))
		}(&wg, i)
	}
	wg.Wait()
}

func CleanData(client S3.S3Client, numOfObjects int) {
	PrintlnC.Cyan("Starting to clear the data")
	wg := sync.WaitGroup{}
	wg.Add(numOfObjects)
	for i := 0; i < numOfObjects; i++ {
		go func(syncer *sync.WaitGroup, index int) {
			defer syncer.Done()
			client.DeleteObject(fmt.Sprintf(FileName, index))
		}(&wg, i)
	}
	wg.Wait()
}

func initFlags() {
	examples := []string{"--AccessKey=1234 --SecretKey=1234 --Endpoint=https://s3.us-east-1.amazonaws.com --Bucket=bari --Region=us-east-1 --Put",
		"--AccessKey=1234 --SecretKey=1234 --Endpoint=https://s3.us-east-1.amazonaws.com --Bucket=bari --Region=us-east-1 --Get",
		"--AccessKey=1234 --SecretKey=1234 --Endpoint=https://s3.us-east-1.amazonaws.com --Bucket=bari --Region=us-east-1 --Put --Clean",
		"--AccessKey=1234 --SecretKey=1234 --Endpoint=https://s3.us-east-1.amazonaws.com --Bucket=bari --Region=us-east-1 --Get --Clean",
	}
	flag.Usage = usage([]string{"--AccessKey=<value> --SecretKey=<value> --Endpoint=<value> --Bucket=<value> --Region=<value> --Put/--Get"}, examples)
	flag.IntVar(&NumOfObjects, "NumOfObjects", 10000, "Number of objects")
	flag.BoolVar(&Put, "Put", false, "Put objects load")
	flag.BoolVar(&Get, "Get", false, "Get objects load")
	flag.BoolVar(&Clean, "Clean", false, "Do not clean the bucket after running the load")
	flag.StringVar(&Config.BucketName, "Bucket", "", "The bucket name")
	flag.StringVar(&Config.AccessKey, "AccessKey", "", "The access key")
	flag.StringVar(&Config.SecretKey, "SecretKey", "", "The secret key")
	flag.StringVar(&Config.Endpoint, "Endpoint", "", "The endpoint")
	flag.StringVar(&Config.Region, "Region", "us-east-1", "The bucket region")
	flag.Parse()
}

func verifyFlags() error {
	if Config.AccessKey == "" {
		return errors.New("ERROR: --AccessKey flag cannot be empty! Please provide the access key\n")
	}
	if Config.SecretKey == "" {
		return errors.New("ERROR: --SecretKey flag cannot be empty! Please provide the secret key\n")
	}
	if Config.Endpoint == "" {
		return errors.New("ERROR: --Endpoint flag cannot be empty! Please provide the endpoint\n")
	}
	if Config.BucketName == "" {
		return errors.New("ERROR: --BucketName flag cannot be empty! Please provide the bucket name\n")
	}
	if Put && Get {
		return errors.New("ERROR: --Put and --Get flags cannot be set at once! Please provide one of them\n")
	}
	if !Put && !Get {
		return errors.New("ERROR: you must set one of the flags to use this tool. Please provide one of them: --Put or --Get\n")
	}
	return nil
}

// Usage prints a usage message documenting all defined command-line flags in a nice way formatted so u can use it to override
// the default flag.Usage func also added the to set the Usage and Example print so the Usage Msg will look much clear
func usage(mustUseFlags, examples []string) func() {
	return func() {
		f := flag.CommandLine
		if len(mustUseFlags) > 0 {
			fmt.Println("Usage:")
			for _, useFlag := range mustUseFlags {
				fmt.Fprintf(f.Output(), "  %s %s\n", os.Args[0], useFlag)
			}
		}
		if len(examples) > 0 {
			fmt.Println("Example:")
			for _, example := range examples {
				fmt.Fprintf(f.Output(), "  %s %s\n", os.Args[0], example)
			}
			fmt.Println()
		}
		fmt.Fprintf(f.Output(), "All optional flag's:\n")
		writer := tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', tabwriter.AlignRight)
		defer writer.Flush()
		flag.VisitAll(func(flag_ *flag.Flag) {
			if flag_.Usage == "" {
				return
			}
			s := fmt.Sprintf("  --%s", flag_.Name)
			name, usage := flag.UnquoteUsage(flag_)
			if len(name) > 0 {
				s += " " + name
			}
			usage = strings.ReplaceAll(usage, "\n", "")
			if flag_.DefValue != "" {
				usage += fmt.Sprintf(" (default %v)", flag_.DefValue)
			}
			_, _ = fmt.Fprintf(writer, "%s\t    %s\t\n", s, usage)
		})
	}
}
