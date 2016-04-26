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
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/codegangsta/cli"
)

// listFiles lists the files in the bucket
func (r cliCommand) listFiles(o *formater, cx *cli.Context) error {
	// step: get the bucket name
	bucket := cx.String("bucket")
	detailed := cx.Bool("long")
	recursive := cx.Bool("recursive")

	// step: get the paths to iterate
	for _, p := range r.getPaths(cx) {
		// step: get a list of paths down that path
		files, err := r.listBucketKeys(bucket, p)
		if err != nil {
			return err
		}

		// step: iterate the files
		for _, k := range files {
			// step: are we recursive? i.e. extract post prefix and ignore any keys which have a / in them
			if strings.Contains(strings.TrimPrefix(*k.Key, p), "/") && !recursive {
				continue
			}
			// step: are we performing a detailed listing?
			switch detailed {
			case true:
				o.fields(map[string]interface{}{
					"key":           *k.Key,
					"size":          *k.Size,
					"class":         *k.StorageClass,
					"etag":          *k.ETag,
					"owner":         *k.Owner,
					"last-modified": k.LastModified,
				}).log("%s %-10d %-20s %s\n", *k.Owner.DisplayName, *k.Size, (*k.LastModified).Format(time.RFC822), *k.Key)
			default:
				o.fields(map[string]interface{}{
					"key": *k.Key,
				}).log("%s\n", *k.Key)
			}
		}
	}

	return nil
}

// catFiles display one of more files to the screen
func (r cliCommand) catFiles(o *formater, cx *cli.Context) error {
	bucket := cx.String("bucket")

	for _, filename := range cx.Args() {
		content, err := r.getFileBlob(bucket, filename)
		if err != nil {
			return err
		}

		fmt.Fprintf(os.Stdout, "%s", content)
	}

	return nil
}

// getFiles retrieve files from bucket
func (r cliCommand) getFiles(o *formater, cx *cli.Context) error {
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
	for _, p := range r.getPaths(cx) {
		// step: drop the slash to for empty
		if strings.HasPrefix(p, "/") {
			p = strings.TrimPrefix(p, "/")
		}

		// step: list all the keys in the bucket
		keys, err := r.listBucketKeys(bucket, p)
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
			content, err := r.getFileBlob(bucket, *k.Key)
			if err != nil {
				return err
			}
			// step: are we flattening the files
			if strings.Contains(filename, "/") && flatten {
				filename = fmt.Sprintf("%s/%s", outdir, filepath.Base(filename))
			}
			// step: ensure the directory structure
			fullPath := fmt.Sprintf("%s/%s", outdir, filename)
			if err := os.MkdirAll(outdir + "/" + path.Dir(filename), 0755); err != nil {
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

// putFiles uploads a selection of files into the bucket
func (r cliCommand) putFiles(o *formater, cx *cli.Context) error {
	// step: grab the options
	bucket := cx.String("bucket")
	kms := cx.String("kms")
	flatten := cx.Bool("flatten")

	// step: ensure the bucket exists
	if found, err := r.hasBucket(bucket); err != nil {
		return err
	} else if !found {
		return fmt.Errorf("the bucket: %s does not exist", bucket)
	}

	// check: we need any least one argument
	if len(cx.Args()) <= 0 {
		return fmt.Errorf("you have not specified any files to upload")
	}

	// step: iterate the paths and upload the files
	for _, p := range r.getPaths(cx) {
		// step: get a list of files under this path
		files, err := expandFiles(p)
		if err != nil {
			return fmt.Errorf("failed to process path: %s, error: %s", p, err)
		}
		// step: iterate the files in the path
		for _, filename := range files {
			// step: construct the key for this file
			keyName := filename
			if flatten {
				keyName = path.Base(keyName)
			}

			// step: upload the file to the bucket
			if err := r.putFile(bucket, keyName, filename, kms); err != nil {
				return fmt.Errorf("failed to put the file: %s, error: %s", filename, err)
			}

			// step: add the log
			o.fields(map[string]interface{}{
				"action": "put",
				"path":   filename,
				"bucket": bucket,
				"key":    keyName,
			}).log("successfully pushed the file: %s to s3://%s/%s\n", filename, bucket, keyName)
		}
	}

	return nil
}

// editFile permits an inline edit of the file
func (r cliCommand) editFile(o *formater, cx *cli.Context) error {
	bucket := cx.String("bucket")

	// step: get the editor
	editor := "vim"
	if os.Getenv("EDITOR") != "" {
		editor = os.Getenv("EDITOR")
	}

	for _, x := range cx.Args() {
		// step: attempt to retrieve the data
		content, err := r.getFileBlob(bucket, x)
		if err != nil {
			return fmt.Errorf("unable to retrieve the file: %s, error: %s", x, err)
		}
		// step: write the file to the
		if err := inlineEditFile(x, content, editor); err != nil {
			return fmt.Errorf("unable to edit the file: %s, error: %s", x, err)
		}
	}

	return nil
}

// inlineEditFile performs an inline edit of the file
func inlineEditFile(filename string, content []byte, editor string) error {
	// step: create a temporary file and write the data
	tmpfile, err := ioutil.TempFile("/tmp", filename+".XXXXXXXX")
	if err != nil {
		return err
	}
	defer func() {
		// delete the file
		os.Remove(tmpfile.Name())
	}()
	if _, err := tmpfile.Write(content); err != nil {
		return err
	}
	tmpfile.Close()


	// step: open the secret with the editor
	cmd := exec.Command(editor, tmpfile.Name())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}

	return nil
}


// getFileBlob retrieves the content from a file in the bucket
func (r cliCommand) getFileBlob(bucket, key string) ([]byte, error) {
	// step: build the input options
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	// step: retrieve the object from the bucket
	resp, err := r.s3Client.GetObject(input)
	if err != nil {
		return nil, err
	}
	// step: read the content
	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return content, nil
}

// hasKey checks if the key exist in the bucket
func (r cliCommand) hasKey(key, bucket string) (bool, error) {
	keys, err := r.listBucketKeys(bucket, filepath.Dir(key))
	if err != nil {
		return false, err
	}

	for _, k := range keys {
		if key == *k.Key {
			return true, nil
		}
	}

	return false, nil
}

// hasBucket checks if the bucket exists
func (r cliCommand) hasBucket(bucket string) (bool, error) {
	list, err := r.listS3Buckets()
	if err != nil {
		return false, err
	}
	for _, x := range list {
		if bucket == *x.Name {
			return true, nil
		}
	}

	return false, nil
}

// putFile uploads a file to the bucket
func (r cliCommand) putFile(bucket, key, path, kmsID string) error {
	// step: open the file
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	// step: upload the file
	_, err = r.uploader.Upload(&s3manager.UploadInput{
		Bucket:               aws.String(bucket),
		Key:                  aws.String(key),
		Body:                 file,
		ServerSideEncryption: aws.String("aws:kms"),
		SSEKMSKeyId:          aws.String(kmsID),
	})

	return err
}

// listBucketKeys get all the keys from the bucket
func (r cliCommand) listBucketKeys(bucket, prefix string) ([]*s3.Object, error) {
	var list []*s3.Object

	resp, err := r.s3Client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, err
	}

	// step: filter out any keys which are directories
	for _, x := range resp.Contents {
		if strings.HasSuffix(*x.Key, "/") {
			continue
		}
		list = append(list, x)
	}

	return list, nil
}

// sizeOfBucket gets the number of objects in the bucket
func (r cliCommand) sizeOfBucket(name string) (int, error) {
	files, err := r.listBucketKeys(name, "")
	if err != nil {
		return 0, err
	}

	return len(files), nil
}
