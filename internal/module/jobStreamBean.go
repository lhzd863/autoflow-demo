package module

type MetaJobStreamBean struct {
	StreamSys string `yaml:"streamsys"`
	StreamJob string `yaml:"streamjob"`
	Sys       string `yaml:"sys"`
	Job       string `yaml:"job"`
	Enable    string `yaml:"enable"`
}
