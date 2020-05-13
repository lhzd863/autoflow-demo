package module

type MetaSlv struct {
	Apiversion      string `yaml:"apiversion"`
	Name            string `yaml:"name"`
	Port            string `yaml:"port"`
	SendMailCmd     string `yaml:"sendmailcmd"`
	JwtKey          string `yaml:"jwtkey"`
	BboltDBPath     string `yaml:"bboltdbpath"`
        LogFPath        string `yaml:"logfpath"`
}
