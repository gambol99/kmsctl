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
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/codegangsta/cli"
)

func (r cliCommand) listBuckets(o *formater, cx *cli.Context) error {
	// step: get a list of buckets
	buckets, err := r.listS3Buckets()
	if err != nil {
		return err
	}

	// step: produce the entries
	for _, x := range buckets {
		o.fields(map[string]interface{}{
			"created": (*x.CreationDate).Format(time.RFC822Z),
			"bucket":  *x.Name,
		}).log("%-42s %20s\n", *x.Name, (*x.CreationDate).Format(time.RFC822))
	}

	return nil
}

func (r cliCommand) createBucket(o *formater, cx *cli.Context) error {
	name := cx.String("name")

	if found, err := r.hasBucket(name); err != nil {
		return err
	} else if found {
		return fmt.Errorf("the bucket already exists")
	}

	if _, err := r.s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(name),
	}); err != nil {
		return err
	}

	o.fields(map[string]interface{}{
		"operation": "created",
		"bucket":    name,
		"created":   time.Now().Format(time.RFC822Z),
	}).log("successfully created the bucket: %s\n", name)

	return nil
}

func (r cliCommand) deleteBucket(o *formater, cx *cli.Context) error {
	name := cx.String("name")
	force := cx.Bool("force")

	// step: check the bucket exists
	found, err := r.hasBucket(name)
	if err != nil {
		return err
	} else if !found {
		return fmt.Errorf("the bucket does not exist")
	}

	// step: check if the bucket is empty
	count, err := r.sizeOfBucket(name)
	if err != nil {
		return err
	} else if count > 0 && !force {
		return fmt.Errorf("the bucket is not empty, either force (--force) deletion or empty the bucket")
	}

	// step: delete all the keys in the bucket first
	// @TODO find of there is a force deletion api call
	if count > 0 {
		files, err := r.listBucketKeys(name, "")
		if err != nil {
			return err
		}
		for _, x := range files {
			if _, err := r.s3Client.DeleteObject(&s3.DeleteObjectInput{
				Bucket: aws.String(name),
				Key:    x.Key,
			}); err != nil {
				return fmt.Errorf("failed to remove the file: %s from bucket, error: %s", *x.Key, err)
			}
		}
	}
	// step: delete the bucket
	if _, err := r.s3Client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(name),
	}); err != nil {
		return err
	}

	o.fields(map[string]interface{}{
		"operation": "delete",
		"bucket":    name,
		"created":   time.Now().Format(time.RFC822Z),
	}).log("successfully deleted the bucket: %s\n", name)

	return nil
}

// listS3Buckets gets a list of buckets
func (r cliCommand) listS3Buckets() ([]*s3.Bucket, error) {
	list, err := r.s3Client.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	return list.Buckets, nil
}
