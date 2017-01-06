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
	"strings"
	"errors"

	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/urfave/cli"
	"github.com/aws/aws-sdk-go/aws"
)

var errKmsNotFound = errors.New("kms alias does not exist")

//
// newKMSCommand creates a new list kms key command
//
func newKMSCommand(cmd *cliCommand) cli.Command {
	return cli.Command{
		Name:      "kms",
		Usage:     "provides the ability to create, list and delete kms keys",
		Subcommands: []cli.Command{
			{
				Name:  "ls, list",
				Usage: "retrieve a listing of all the kms within the specified region",
				Action: func(cx *cli.Context) error {
					return handleCommand(cx, []string{}, cmd, listKeys)
				},
			},
			{
				Name:  "create",
				Usage: "create a ksm key in the specified region",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "n, name",
						Usage: "the name of the kms key you wish to create `NAME`",
					},
					cli.StringFlag{
						Name:  "d, description",
						Usage: "the description of the kms key you wish to create `DESCRIPTION`",
					},
				},
				Action: func(cx *cli.Context) error {
					return handleCommand(cx, []string{"l:name:s","l:description:s"}, cmd, createKey)
				},
			},
			{
				Name:    "delete",
				Aliases: []string{"rm"},
				Usage:   "delete a kms key in the specified region",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:  "n, name",
						Usage: "the name of the kms key you wish to delete `NAME`",
					},
					cli.BoolTFlag{
						Name:  "schedule-deletion",
						Usage: "indicates if you wish to schedule the key for deletion `BOOL`",
					},
				},
				Action: func(cx *cli.Context) error {
					return handleCommand(cx, []string{"l:name:s"}, cmd, deleteKey)
				},
			},
		},
		Action: func(cx *cli.Context) error {
			return handleCommand(cx, []string{}, cmd, listKeys)
		},
	}
}

//
// listKeys provides a listing of kms keys available
//
func listKeys(o *formatter, cx *cli.Context, cmd *cliCommand) error {
	// step: retrieve the keys from kms
	keys, err := cmd.kmsKeys()
	if err != nil {
		return err
	}

	// step: produce a listing
	for _, k := range keys {
		// step: skip any kms keys which do not have an id
		if k.TargetKeyId == nil {
			continue
		}
		o.fields(map[string]interface{}{
			"id":    *k.TargetKeyId,
			"alias": *k.AliasName,
		}).log("%-40s %-24s\n", *k.TargetKeyId, *k.AliasName)
	}

	return nil
}

//
// createKey provides the ability to create a kms key
//
func createKey(o *formatter, cx *cli.Context, cmd *cliCommand) error {
	name := cx.String("name")
	description := cx.String("description")
	aliasName := fmt.Sprintf("alias/%s", name)

	// step: check if a key already exists
	exists, err := cmd.hasKmsAlias(name)
	if err != nil {
		return err
	}
	if exists {
		o.fields(map[string]interface{}{
			"alias":    name,
		}).log("a key alias already exists with this name: %s\n", name)

		return nil
	}

	// step: create the key creation input
	input := &kms.CreateKeyInput{
		Description: 	aws.String(description),
		Origin: 	aws.String("AWS_KMS"),
	}
	resp, err := cmd.kmsClient.CreateKey(input)
	if err != nil {
		return err
	}

	// step: create the alias for the key
	_, err = cmd.kmsClient.CreateAlias(&kms.CreateAliasInput{
		AliasName: 	aws.String(aliasName),
		TargetKeyId: 	resp.KeyMetadata.Arn,
	})
	if err != nil {
		return err
	}

	o.fields(map[string]interface{}{
		"alias":    name,
		"arn":	    *resp.KeyMetadata.Arn,
		"account":  *resp.KeyMetadata.AWSAccountId,
	}).log("successfully create the key: %s\n", name)

	return nil
}

//
// deleteKey removes a kms key
//
func deleteKey(o *formatter, cx *cli.Context, cmd *cliCommand) error {
	name := cx.String("name")
	deletion := cx.Bool("schedule-deletion")

	// step: get a list of the aliases
	alias, err := cmd.getKmsAlias(name)
	if err != nil {
		return err
	}
	// step: attempt to remove the alias
	if _, err = cmd.kmsClient.DeleteAlias(&kms.DeleteAliasInput{
		AliasName: alias.AliasName,
	}); err != nil {
		return err
	}
	// step: are we deleting the key?
	if deletion {
		// step: attempt to schedule to the removal of the key
		if _, err = cmd.kmsClient.ScheduleKeyDeletion(&kms.ScheduleKeyDeletionInput{
			KeyId: 			aws.String(*alias.TargetKeyId),
			PendingWindowInDays: 	aws.Int64(7),
		}); err != nil {
			return err
		}
	}

	o.fields(map[string]interface{}{
		"alias":	*alias.AliasName,
		"keyId":	*alias.TargetKeyId,
		"deletion":	deletion,
	}).log("successfully deleted the kms key: %s\n", name)

	return nil
}

//
// hasKmsAlias checks to see if an alias already exists
//
func (r *cliCommand) hasKmsAlias(name string) (bool, error) {
	_, err := r.getKmsAlias(name)
	if err != nil {
		if err == errKmsNotFound {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

//
// getKmsAlias retrieves a specific key alias
//
func (r *cliCommand) getKmsAlias(name string) (*kms.AliasListEntry, error) {
	aliases, err := r.kmsKeys()
	if err != nil {
		return nil, err
	}

	var alias *kms.AliasListEntry
	for _, x := range aliases {
		if strings.TrimLeft(*x.AliasName, "alias/") == name {
			alias = x
			break
		}
	}
	if alias == nil {
		return nil, errKmsNotFound
	}

	return alias, err
}

//
// kmsKeys retrieves the kms keys from aws
//
func (r *cliCommand) kmsKeys() ([]*kms.AliasListEntry, error) {
	resp, err := r.kmsClient.ListAliases(&kms.ListAliasesInput{})
	if err != nil {
		return []*kms.AliasListEntry{}, err
	}

	return resp.Aliases, nil
}
