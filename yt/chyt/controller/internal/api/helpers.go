package api

import (
	"fmt"
	"math/rand"
	"reflect"
	"regexp"

	"golang.org/x/exp/slices"

	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func validateStringParameter(pattern string, value string) error {
	matched, err := regexp.MatchString(pattern, value)
	if err != nil {
		return err
	}
	if !matched {
		return yterrors.Err(fmt.Sprintf("%q does not match regular expression %q", value, pattern))
	}
	return nil
}

func validateAlias(alias any) error {
	return validateStringParameter(`^[A-Za-z][\w-]*$`, alias.(string))
}

func validateOption(option any) error {
	return validateStringParameter(`^[A-Za-z][\w./-]*$`, option.(string))
}

func unexpectedTypeError(typeName string) error {
	return yterrors.Err(
		fmt.Sprintf("parameter has unexpected value type %v", typeName),
		yterrors.Attr("type", typeName))
}

func validateSpecletOptions(speclet any) error {
	_, ok := speclet.(map[string]any)
	if !ok {
		typeName := reflect.TypeOf(speclet).String()
		return unexpectedTypeError(typeName)
	}
	return nil
}

func validateSecrets(secrets any) error {
	secretsMap, ok := secrets.(map[string]any)
	if !ok {
		typeName := reflect.TypeOf(secrets).String()
		return unexpectedTypeError(typeName)
	}
	allowedTypes := []string{"string", "int64", "uint64", "bool", "float64"}
	for _, v := range secretsMap {
		if vType := reflect.TypeOf(v).String(); !slices.Contains(allowedTypes, vType) {
			return unexpectedTypeError(vType)
		}
	}
	return nil
}

func validateBool(value any) error {
	_, ok := value.(bool)
	if !ok {
		typeName := reflect.TypeOf(value).String()
		return unexpectedTypeError(typeName)
	}
	return nil
}

func transformToStringSlice(value any) (any, error) {
	if value == nil {
		return []string(nil), nil
	}

	array, ok := value.([]any)
	if !ok {
		typeName := reflect.TypeOf(value).String()
		return nil, unexpectedTypeError(typeName)
	}

	transformedAttributes := []string{}
	for _, element := range array {
		if _, ok = element.(string); !ok {
			typeName := reflect.TypeOf(value).String()
			return nil, unexpectedTypeError(typeName)
		}
		transformedAttributes = append(transformedAttributes, element.(string))
	}

	return transformedAttributes, nil
}

func getCreateSecretNodeOptions(secrets map[string]any, txOptions *yt.TransactionOptions) *yt.CreateNodeOptions {
	return &yt.CreateNodeOptions{
		Attributes: map[string]any{
			"value": secrets,
			"acl": []yt.ACE{
				{
					Action:   yt.ActionAllow,
					Subjects: []string{"owner"},
					Permissions: []yt.Permission{
						yt.PermissionAdminister,
						yt.PermissionRead,
						yt.PermissionWrite,
						yt.PermissionRemove,
					},
				},
			},
			"inherit_acl": false,
		},
		Force:              true,
		TransactionOptions: txOptions,
	}
}

func getRandomString(length int) string {
	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
