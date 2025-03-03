package tryt

import "fmt"

type Config struct {
	ExtraEnvVars map[string]string `yson:"extra_env_vars"`
	Command      *string           `yson:"command"`
	LogLevel     *string           `yson:"log_level"`
	LogConfig    *string           `yson:"log_config"`
	JobCount     *int              `yson:"job_count"`
	Binary       *string           `yson:"binary_path"`
	ExtraLayers  []string          `yson:"extra_layers"`
}

func (c *Config) CommandOrDefault(spec Speclet) string {
	if c.Command != nil {
		return *c.Command
	}
	cliPath := "/usr/local/bin/trcli"
	if c.Binary != nil {
		cliPath = "trcli"
	}
	return fmt.Sprintf("%s %s --transfer transfer.yaml --log-level %s --log-config %s", cliPath, spec.Command(), c.LogLevelOrDefault(), c.LogConfigOrDefault())
}

func (c *Config) EnvVars() map[string]string {
	res := map[string]string{
		"NB_GID":        "0",
		"NB_UID":        "0",
		"NB_USER":       "root",
		"YT_BASE_LAYER": "auto",
	}
	for k, v := range c.ExtraEnvVars {
		res[k] = v
	}
	return res
}

func (c *Config) LogLevelOrDefault() string {
	if c.LogLevel != nil {
		return *c.LogLevel
	}
	return "info"
}

func (c *Config) LogConfigOrDefault() string {
	if c.LogConfig != nil {
		return *c.LogConfig
	}
	return "console"
}

func (c *Config) JobCountOrDefault() int {
	if c.JobCount != nil {
		return *c.JobCount
	}
	return 1
}
