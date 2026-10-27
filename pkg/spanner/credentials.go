package spanner

import (
	"fmt"

	"google.golang.org/api/option"
)

func credentialsClientOption(config *Config) (option.ClientOption, error) {
	if config.CredentialsFile == "" {
		return nil, nil
	}

	credentialsType, err := resolveCredentialsType(config.CredentialsType)
	if err != nil {
		return nil, err
	}
	return option.WithAuthCredentialsFile(credentialsType, config.CredentialsFile), nil
}

func resolveCredentialsType(credentialsType option.CredentialsType) (option.CredentialsType, error) {
	if credentialsType == "" {
		return option.ServiceAccount, nil
	}

	switch credentialsType {
	case option.ServiceAccount, option.AuthorizedUser:
		return credentialsType, nil
	default:
		return "", fmt.Errorf(
			"unsupported credentials type %q; use %q or %q",
			credentialsType,
			option.ServiceAccount,
			option.AuthorizedUser,
		)
	}
}
