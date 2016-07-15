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
	"path/filepath"
	"regexp"
	"strings"

	"github.com/urfave/cli"
)

//
// newGetCommand creates a new get command
//
func newGetCommand(cmd *cliCommand) cli.Command {
	return cli.Command{
		Name:  "get",
		Usage: "retrieve one or more files from the s3 bucket",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:   "b, bucket",
				Usage:  "the name of the s3 bucket containing the encrypted files",
				EnvVar: "AWS_S3_BUCKET",
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
				Name:   "d, output-dir",
				Usage:  "the path to the directory in which to save the files",
				EnvVar: "KMSCTL_OUTPUT_DIR",
				Value:  "./secrets",
			},
			cli.StringFlag{
				Name:  "f, filter",
				Usage: "apply the following regex filter to the files before retrieving",
				Value: ".*",
			},
		},
		Action: func(cx *cli.Context) error {
			return handleCommand(cx, []string{"l:bucket", "l:output-dir"}, cmd, getFiles)
		},
	}
}

//
// getFiles retrieve files from bucket
//
func getFiles(o *formatter, cx *cli.Context, cmd *cliCommand) error {
	// step: get the inputs
	bucket := cx.String("bucket")
	outdir := cx.String("output-dir")
	flatten := cx.Bool("flatten")
	recursive := cx.Bool("recursive")

	// step: validate the filter if any
	filter, err := regexp.Compile(cx.String("filter"))
	if err != nil {
		return fmt.Errorf("the filter: %s is invalid, message: %s", cx.String("filter"), err)
	}

	// step: create the output directory if required
	if err := os.MkdirAll(outdir, 0755); err != nil {
		return err
	}

	// step: iterate the paths build a list of files were interested in
	for _, p := range getPaths(cx) {
		// step: drop the slash to for empty
		if strings.HasPrefix(p, "/") {
			p = strings.TrimPrefix(p, "/")
		}

		// step: list all the keys in the bucket
		keys, err := cmd.listBucketKeys(bucket, p)
		if err != nil {
			return err
		}
		// step: iterate the files
		for _, k := range keys {
			filename := *k.Key

			// step: are we recursive? i.e. extract post prefix and ignore any keys which have a / in them
			if strings.Contains(filename, "/") && !recursive {
				continue
			}
			// step: apply the filter
			if !filter.MatchString(*k.Key) {
				continue
			}
			// step: retrieve the file content
			content, err := cmd.getFile(bucket, *k.Key)
			if err != nil {
				return err
			}
			// step: are we flattening the files
			if strings.Contains(filename, "/") && flatten {
				filename = fmt.Sprintf("%s/%s", outdir, filepath.Base(filename))
			}
			// step: ensure the directory structure
			fullPath := fmt.Sprintf("%s/%s", outdir, filename)
			if err := os.MkdirAll(outdir+"/"+filepath.Dir(filename), 0755); err != nil {
				return err
			}
			// step: create the file for writing
			file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0744)
			if err != nil {
				return err
			}
			if _, err := file.Write(content); err != nil {
				return err
			}

			// step: add the log
			o.fields(map[string]interface{}{
				"action":      "get",
				"source":      *k.Key,
				"destination": fullPath,
				"content":     string(content),
			}).log("retrieved the file: %s and wrote to: %s\n", *k.Key, fullPath)
		}
	}

	return nil
}
