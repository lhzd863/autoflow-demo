package module

type MetaApiServerBean struct {
	Apiversion    string `yaml:"apiversion"`
	Name          string `yaml:"name"`
	Port          string `yaml:"port"`
	JwtKey        string `yaml:"jwtkey"`
	BboltDBPath   string `yaml:"bboltdbpath"`
	LogFPath      string `yaml:"logfpath"`
	HomeDir       string `yaml:"homedir"`
	MstIp         string `yaml:"mstip"`
	MstPort       string `yaml:"mstport"`
	MstMaxPort    int64  `yaml:"mstmaxport"`
	MstMinPort    int64  `yaml:"mstminport"`
	ApiServerIp   string `yaml:"apiserverip"`
	ApiServerPort string `yaml:"apiserverport"`
        AccessToken   string `yaml:"accesstoken"`
}
