/*
Copyright 2015 All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/codegangsta/cli"
)

type cliCommand struct {
	// the kms client for aws
	kmsClient *kms.KMS
	// the s3 client
	s3Client *s3.S3
	// the s3 uploader
	uploader *s3manager.Uploader
}

func newCliApplication() *cli.App {
	r := new(cliCommand)
	app := cli.NewApp()
	app.Name = progName
	app.Usage = "is a utility for interacting to s3 and kms encrypted files"
	app.Author = author
	app.Version = version
	app.Email = email
	app.Flags = r.getGlobalOptions()
	app.Before = r.getCredentials()

	app.Commands = []cli.Command{
		{
			Name:  "kms",
			Usage: "provide a listing of the kms key presently available to us",
			Action: func(cx *cli.Context) {
				r.handleCommand(cx, []string{}, r.listKeys)
			},
		},
		{
			Name:  "buckets",
			Usage: "provides a list of the buckets available to you",
			Subcommands: []cli.Command{
				{
					Name:  "ls, list",
					Usage: "retrieve a listing of all the buckets within the specified region",
					Action: func(cx *cli.Context) {
						r.handleCommand(cx, []string{}, r.listBuckets)
					},
				},
				{
					Name:  "create",
					Usage: "create a bucket in the specified region",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "n, name",
							Usage: "the name of the bucket you wish to create",
						},
					},
					Action: func(cx *cli.Context) {
						r.handleCommand(cx, []string{"l:name"}, r.createBucket)
					},
				},
				{
					Name:    "delete",
					Aliases: []string{"rm"},
					Usage:   "delete a bucket in the specified region",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "n, name",
							Usage: "the name of the bucket you wish to delete",
						},
						cli.BoolFlag{
							Name:  "force",
							Usage: "delete the bucket regardless if empty or not",
						},
					},
					Action: func(cx *cli.Context) {
						r.handleCommand(cx, []string{"l:name"}, r.deleteBucket)
					},
				},
			},
			Action: func(cx *cli.Context) {
				r.handleCommand(cx, []string{}, r.listBuckets)
			},
		},
		{
			Name:    "list",
			Aliases: []string{"ls"},
			Usage:   "providing a file listing of the files currently in there",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "l, long",
					Usage: "provide a detailed / long listing of the files in the bucket",
				},
				cli.StringFlag{
					Name:   "b, bucket",
					Usage:  "the name of the s3 bucket containing the encrypted files",
					EnvVar: "AWS_SECRETS_BUCKET",
				},
				cli.BoolTFlag{
					Name:  "r, recursive",
					Usage: "enable recursive option and transverse all subdirectories",
				},
			},
			Action: func(cx *cli.Context) {
				r.handleCommand(cx, []string{"l:bucket"}, r.listFiles)
			},
		},
		{
			Name:  "get",
			Usage: "retrieve one or more files from the s3 bucket",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "b, bucket",
					Usage:  "the name of the s3 bucket containing the encrypted files",
					EnvVar: "AWS_SECRETS_BUCKET",
				},
				cli.StringFlag{
					Name:  "p, perms",
					Usage: "the file permissions on any newly created files",
					Value: "0744",
				},
				cli.BoolFlag{
					Name:  "r, recursive",
					Usage: "enable recursive option and transverse all subdirectories",
				},
				cli.BoolFlag{
					Name:  "flatten",
					Usage: "do not maintain the directory structure, flattern all files into a single directory",
				},

				cli.StringFlag{
					Name:  "f, filter",
					Usage: "apply the following regex filter to the files before retrieving",
					Value: ".*",
				},
			},
			Action: func(cx *cli.Context) {
				r.handleCommand(cx, []string{"l:bucket", "g:output-dir"}, r.getFiles)
			},
		},
		{
			Name:  "cat",
			Usage: "retrieves and displays the contents of one or more files to the stdout",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "b, bucket",
					Usage:  "the name of the s3 bucket containing the encrypted files",
					EnvVar: "AWS_SECRETS_BUCKET",
				},
			},
			Action: func(cx *cli.Context) {
				r.handleCommand(cx, []string{"l:bucket"}, r.catFiles)
			},
		},
		{
			Name:  "put",
			Usage: "upload one of more files, encrypt and place into the bucket",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "b, bucket",
					Usage:  "the name of the s3 bucket containing the encrypted files",
					EnvVar: "AWS_SECRETS_BUCKET",
				},
				cli.StringFlag{
					Name:   "k, kms",
					Usage:  "the aws kms id to use when performing operations",
					EnvVar: "AWS_KMS_ID",
				},
				cli.BoolFlag{
					Name:  "flatten",
					Usage: "do not maintain the directory structure, flatten all files into a single directory",
				},
			},
			Action: func(cx *cli.Context) {
				r.handleCommand(cx, []string{"l:bucket", "l:kms"}, r.putFiles)
			},
		},
		{
			Name:  "edit",
			Usage: "perform an inline edit of a file either locally or from s3 bucket",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "b, bucket",
					Usage:  "the name of the s3 bucket containing the encrypted files",
					EnvVar: "AWS_SECRETS_BUCKET",
				},
				cli.BoolFlag{
					Name:  "l, local-file",
					Usage: "indicate the file is locally stored rather than a s3 bucket",
				},
			},
			Action: func(cx *cli.Context) {
				r.handleCommand(cx, []string{"l:bucket"}, r.editFile)
			},
		},
	}

	return app
}

// handleCommand is a generic wrapper for handling commands, or more precisely their errors
func (r cliCommand) handleCommand(cx *cli.Context, options []string, method func(*formater, *cli.Context) error) {
	// step: handle any panics in the command
	/*defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[error] internal error occurred, message: %s", r)
			os.Exit(1)
		}
	}()
	*/

	// step: check the required options were passed
	for _, k := range options {
		items := strings.Split(k, ":")
		if len(items) != 2 {
			panic("invalid required option definition, TYPE:NAME")
		}
		switch otype := items[0]; otype {
		case "g":
			if !cx.GlobalIsSet(items[1]) {
				printError("the global option: '%s' is required for this command", items[1])
			}
		default:
			if !cx.IsSet(items[1]) {
				printError("the command option '%s' is required", items[1])
			}
		}
	}

	// step: create a cli output
	writer, err := newFormater(cx.GlobalString("format"), os.Stdout)
	if err != nil {
		printError("error: %s", err)
	}

	// step: call the command and handle any errors
	if err := method(writer, cx); err != nil {
		printError("operation failed, error: %s", err)
	}
}

func printError(message string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "[error] "+message+"\n", args...)
	os.Exit(1)
}

// getPaths returns a list of paths from the arguments, else default to base
func (r cliCommand) getPaths(cx *cli.Context) []string {
	if len(cx.Args()) <= 0 {
		return []string{""}
	}

	return cx.Args()
}

func (r *cliCommand) getCredentials() func(cx *cli.Context) error {
	return func(cx *cli.Context) error {
		if cx.GlobalString("region") == "" {
			fmt.Fprintf(os.Stderr, "[error] you have not specified the aws region the resources reside\n")
			os.Exit(1)
		}
		config := &aws.Config{
			Region: aws.String(cx.GlobalString("region")),
		}

		// step: are we using static credentials
		if cx.GlobalString("access-key") != "" || cx.GlobalString("secret-ket") != "" {
			if cx.GlobalString("secret-key") == "" {
				return fmt.Errorf("you have specified a access key with a secret key")
			}
			if cx.GlobalString("access-key") == "" {
				return fmt.Errorf("you have specified a secret key with a access key")
			}
			config.Credentials = credentials.NewStaticCredentials(cx.GlobalString("access-key"),
				cx.GlobalString("secret-key"),
				cx.GlobalString("session-token"))
		} else if cx.GlobalString("profile") != "" {
			config.Credentials = credentials.NewSharedCredentials(
				cx.GlobalString("credentials"),
				cx.GlobalString("profile"))

		}

		// step: create the clients
		r.s3Client = s3.New(session.New(config))
		r.kmsClient = kms.New(session.New(config))
		r.uploader = s3manager.NewUploader(session.New(config))

		return nil
	}
}

func (r cliCommand) getGlobalOptions() []cli.Flag {
	return []cli.Flag{
		cli.StringFlag{
			Name:   "p, profile",
			Usage:  "the aws profile to use for static credentials",
			EnvVar: "AWS_DEFAULT_PROFILE",
		},
		cli.StringFlag{
			Name:   "c, credentials",
			Usage:  "the path to the credentials file container the aws profiles",
			EnvVar: "AWS_SHARED_CREDENTIALS_FILE",
			Value:  os.Getenv("HOME") + "/.aws/credentials",
		},
		cli.StringFlag{
			Name:   "access-key",
			Usage:  "the aws access key to use to access the resources",
			EnvVar: "AWS_ACCESS_KEY_ID",
		},
		cli.StringFlag{
			Name:   "secret-key",
			Usage:  "the aws secret key to use when accessing the resources",
			EnvVar: "AWS_SECRET_ACCESS_KEY",
		},
		cli.StringFlag{
			Name:  "o, output-dir",
			Usage: "the path to the directory in which to save the files",
			EnvVar: "KMSCTL_OUTPUT_DIR",
			Value:  "./secrets",
		},
		cli.StringFlag{
			Name:   "session-token",
			Usage:  "the aws session token to use when accessing the resources",
			EnvVar: "AWS_SESSION_TOKEN",
		},
		cli.StringFlag{
			Name:   "r, region",
			Usage:  "the aws region where the resources are located",
			EnvVar: "AWS_DEFAULT_REGION",
			Value:  "eu-west-1",
		},
		cli.StringFlag{
			Name:  "f, format",
			Usage: "the format of the output to generate (accepts json, yaml or default text)",
			Value: "text",
		},
	}
}
