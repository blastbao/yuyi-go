// Copyright 2015 The yuyi Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shared

import (
	"io/ioutil"
	"time"

	"gopkg.in/yaml.v2"
)

// NewConfig creates a new Config from a yaml file.
func NewConfig(fpath string) (*Config, error) {
	b, err := ioutil.ReadFile(fpath)
	if err != nil {
		return nil, err
	}

	configYaml := configYaml{}
	err = yaml.Unmarshal(b, &configYaml)
	if err != nil {
		return nil, err
	}

	return &Config{
		configYaml: configYaml,
	}, nil
}

// Config holds the configuration parameters for building a yuyi-go service
type Config struct {
	configYaml
}

type configYaml struct {
	Name string `yaml:"name"`
	Dir  string `yaml:"data-dir"`

	ChunkConfig chunkConfig `yaml:"chunk"`
}

type chunkConfig struct {
	MaxCapacity   int           `yaml:"max-capacity"`
	MaxBatchSize  int           `yaml:"max-batch-size"`
	MaxBatchDelay time.Duration `yaml:"max-batch-delay"`
}
