package spanner

import "fmt"

type Config struct {
	Project         string
	Instance        string
	Database        string
	CredentialsFile string
}

func (c *Config) URL() string {
	return fmt.Sprintf(
		"projects/%s/instances/%s/databases/%s",
		c.Project,
		c.Instance,
		c.Database,
	)
}
