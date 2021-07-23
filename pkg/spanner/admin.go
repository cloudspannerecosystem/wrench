package spanner

import (
	"context"
	"fmt"

	instancev1 "cloud.google.com/go/spanner/admin/instance/apiv1"
	"google.golang.org/api/option"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
)

type AdminClient struct {
	config                     *Config
	spannerInstanceAdminClient *instancev1.InstanceAdminClient
}

func NewAdminClient(ctx context.Context, config *Config) (*AdminClient, error) {
	opts := make([]option.ClientOption, 0)
	if config.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(config.CredentialsFile))
	}

	instanceAdminClient, err := instancev1.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		return nil, &Error{
			Code: ErrorCodeCreateClient,
			err:  fmt.Errorf("failed to create instance admin client: %w", err),
		}
	}

	return &AdminClient{
		config:                     config,
		spannerInstanceAdminClient: instanceAdminClient,
	}, nil
}

func (c *AdminClient) Close() error {
	if err := c.spannerInstanceAdminClient.Close(); err != nil {
		return &Error{
			err:  fmt.Errorf("failed to close instance admin client: %w", err),
			Code: ErrorCodeCloseClient,
		}
	}

	return nil
}

func (c *AdminClient) CreateInstance(ctx context.Context, node int32) error {
	req := &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", c.config.Project),
		InstanceId: c.config.Instance,
		Instance: &instancepb.Instance{
			Name:        "",
			Config:      "",
			DisplayName: "",
			NodeCount:   node,
		},
	}

	op, err := c.spannerInstanceAdminClient.CreateInstance(ctx, req)
	if err != nil {
		return &Error{
			Code: ErrorCodeTruncateAllTables,
			err:  err,
		}
	}

	_, err = op.Wait(ctx)
	if err != nil {
		return &Error{
			Code: ErrorCodeTruncateAllTables,
			err:  err,
		}
	}

	return nil
}
