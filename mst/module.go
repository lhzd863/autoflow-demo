package mst

type MetaConf struct {
	Apiversion  string `yaml:"apiversion"`
	Name        string `yaml:"name"`
	Port        string `yaml:"port"`
	SendMailCmd string `yaml:"sendmailcmd"`
	JwtKey      string `yaml:"jwtkey"`
	BboltDBPath string `yaml:"bboltdbpath"`
	LogFPath    string `yaml:"logfpath"`
}

type MetaJob struct {
	Sys              string `json:"sys"`
	Job              string `json:"job"`
	Server           string `json:"server"`
	Frequency        string `json:"frequency"`
	JobType          string `json:"jobtype"`
	Priority         int64  `json:"priority"`
	CheckCalendar    string `json:"checkcalendar"`
	CheckLastStatus  string `json:"checklaststatus"`
	CheckTimeTrigger string `json:"checktimetrigger"`
	CheckTimeWindows string `json:"checktimewindows"`
	RetryCnt         string `json:"retrycnt"`
	ErrAlert         string `json:"erralert"`
	Owner            string `json:"owner"`
	Enable           string `json:"enable"`
}

type MetaJobCTL struct {
	BatId                string        `yaml:"batid"`
	Sys                  string        `yaml:"sys"`
	Job                  string        `yaml:"job"`
	Server               string        `yaml:"server"`
	Frequency            string        `yaml:"frequency"`
	JobType              string        `yaml:"jobtype"`
	Priority             int64         `yaml:"priority"`
	CheckCalendar        string        `yaml:"checkcalendar"`
	CheckLastStatus      string        `yaml:"checklaststatus"`
	CheckTimeTrigger     string        `yaml:"checktimetrigger"`
	TimeTriggerExpr      string        `yaml:"timetriggerexpr"`
	CheckTimeWindows     string        `yaml:"checktimewindows"`
	TimeWindowsStartHour string        `yaml:"timewindowsstarthour"`
	TimeWindowsEndHour   string        `yaml:"timewindowsendhour"`
	RetryCnt             string        `yaml:"retrycnt"`
	ErrAlert             string        `yaml:"erralert"`
	Owner                string        `yaml:"owner"`
	Txts                 string        `yaml:"txts"`
	Status               string        `yaml:"status"`
	Cts                  string        `yaml:"cts"`
	Ets                  string        `yaml:"ets"`
	Sid                  string        `yaml:"sid"`
	RunningCmd           string        `yaml:"runningcmd"`
	Para                 []interface{} `yaml:"para"`
	LogContext           string        `yaml:"logcontext"`
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

type MetaJobQueue struct {
	Sys    string `json:"sys"`
	Job    string `json:"job"`
	Txts   string `json:"txts"`
	Cts    string `json:"cts"`
	Enable string `json:"enable"`
}

