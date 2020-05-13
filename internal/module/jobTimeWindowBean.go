package module

type MetaJobTimeWindowBean struct {
	Sys       string `yaml:"sys"`
	Job       string `yaml:"job"`
        Allow     string `yaml:"allow"`
	StartHour int8   `yaml:"starthour"`
	EndHour   int8   `yaml:"endhour"`
	Enable    string `yaml:"enable"`
}
