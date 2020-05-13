package module

type MetaConf struct {
	Apiversion      string `yaml:"apiversion"`
	Name            string `yaml:"name"`
	Port            string `yaml:"port"`
	SendMailCmd     string `yaml:"sendmailcmd"`
	JwtKey          string `yaml:"jwtkey"`
	BboltDBPath     string `yaml:"bboltdbpath"`
        LogFPath        string `yaml:"logfpath"`
        HomeDir         string `yaml:"homedir"`
        ApiServerIp     string `yaml:"apiserverip"`
        ApiServerPort   string `yaml:"apiserverport"`
        MstIp           string `yaml:"mstip"`
        MstMaxPort      int64  `yaml:"mstmaxport"`
        MstMinPort      int64  `yaml:"mstminport"`
}
