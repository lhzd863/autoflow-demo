package slv

type MetaConf struct {
	Apiversion    string `yaml:"apiversion"`
	Name          string `yaml:"name"`
	Ip            string `yaml:"ip"`
	Port          string `yaml:"port"`
	JwtKey        string `yaml:"jwtkey"`
	HomeDir       string `yaml:"homedir"`
	AccessToken   string `yaml:"accesstoken"`
	ApiServerIp   string `yaml:"apiserverip"`
	ApiServerPort string `yaml:"apiserverport"`
        ProcessNum    string `yaml:"processnum"`
}

type MetaJob struct {
	FlowId    string `json:"flowid"`
	Sys       string `json:"sys"`
	Job       string `json:"job"`
	Ip        string `json:"ip"`
	Port      string `json:"port"`
	Frequency string `json:"frequency"`
	JobType   string `json:"jobtype"`
	RetryCnt  string `json:"retrycnt"`
	ErrAlert  string `json:"erralert"`
	Enable    string `json:"enable"`
}

type MetaMyWork struct {
	Id            string `json:"id"`
	HomeDir       string `json:"homedir"`
	FlowId        string `json:"flowid"`
	ApiServerIp   string `json:"apiserverip"`
	ApiServerPort string `json:"apiserverpor`
	LogF          string `json:"logf"`
	MstIp         string `json:"mstip"`
	MstPort       string `json:"mstport"`
	AccessToken   string `json:"accesstoken"`
}
