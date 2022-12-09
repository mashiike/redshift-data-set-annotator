package redshiftdatasetannotator

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/Songmu/prompter"
	"github.com/aws/aws-sdk-go-v2/aws"
)

type Config map[string]*ProfileConfig

type ProfileConfig struct {
	ClusterIdentifier *string `json:"cluster_identifier,omitempty"`
	WorkgroupName     *string `json:"workgroup_name,omitempty"`
	DBUser            *string `json:"db_user,omitempty"`
}

func (cfg Config) String() string {
	b, _ := json.MarshalIndent(cfg, "", "  ")
	return string(b)
}

func (cfg Config) GetDefault() *ProfileConfig {
	return cfg[defaultProfileName]
}

func (cfg Config) Get(host string) (*ProfileConfig, bool) {
	profile, ok := cfg[host]
	return profile, ok
}

const defaultProfileName = "[default]"

func (cfg Config) reConfigure(host string) error {
	profileName := host
	if profileName == "" {
		profileName = defaultProfileName
	}
	profile, ok := cfg[profileName]
	if !ok {
		profile = &ProfileConfig{}
	}
	profile.reConfigure(host)
	cfg[profileName] = profile
	return saveConfig(cfg)
}

func (cfg *ProfileConfig) reConfigure(host string) {
	isP := isProvisoned(host)
	isS := isServeless(host)
	if !isP && !isS {
		if prompter.YesNo(fmt.Sprintf("%s is serverless?:", coalesce(nillif(host, ""), aws.String("default profile"))), false) {
			workgroupName := prompter.Prompt("Enter workgroup name", coalesce(cfg.WorkgroupName, aws.String("default")))
			cfg.WorkgroupName = aws.String(workgroupName)
			cfg.ClusterIdentifier = nil
			cfg.DBUser = nil
			return
		}
	}
	if isS {
		cfg.WorkgroupName = aws.String(
			coalesce(
				cfg.WorkgroupName,
				nillif(getWorkgroupName(host), ""),
				aws.String("default"),
			),
		)
		cfg.ClusterIdentifier = nil
		cfg.DBUser = nil
		return
	}
	cfg.ClusterIdentifier = nillif(
		prompter.Prompt("Enter cluster identifier", coalesce(
			cfg.ClusterIdentifier,
			aws.String(getCluseterID(host)),
		)),
		"",
	)
	cfg.DBUser = nillif(
		prompter.Prompt("Enter db user", coalesce(
			cfg.DBUser,
			aws.String("admin"),
		)),
		"",
	)
}

var configDir string

const configSubdir = "redshift-data-set-annotator"

func init() {
	if h := os.Getenv("XDG_CONFIG_HOME"); h != "" {
		configDir = filepath.Join(h, configSubdir)
	} else {
		d, err := os.UserHomeDir()
		if err != nil {
			d = os.Getenv("HOME")
		}
		configDir = filepath.Join(d, ".config", configSubdir)
	}
}

func newConfig() Config {
	cfg := Config{
		defaultProfileName: &ProfileConfig{
			WorkgroupName: aws.String("default"),
		},
	}
	return cfg
}

func configFilePath() string {
	return filepath.Join(configDir, "config.json")
}

func loadConfigFile() (Config, error) {
	p := configFilePath()
	jsonStr, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return newConfig(), nil
		}
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal([]byte(jsonStr), &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal %s: %w", p, err)
	}
	return cfg, nil
}

func saveConfig(cfg Config) error {
	p := configFilePath()
	if _, err := os.Stat(configDir); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(configDir, 0755); err != nil {
				return fmt.Errorf("failed to create config directory: %w", err)
			}
		} else {
			return fmt.Errorf("failed to stat config directory: %w", err)
		}
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	if _, err := os.Stat(p); err == nil {
		if err := os.Rename(p, p+".bak"); err != nil {
			return fmt.Errorf("failed to backup config: %w", err)
		}
	}
	if err := os.WriteFile(p, b, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}
	log.Println("[info] Saved configuration file:", configFilePath())
	return nil
}
