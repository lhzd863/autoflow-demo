package module

type MetaJobFlowBean struct {
	FlowId        string                 `json:"flowid"`
	ImageId       string                 `json:"imageid"`
	User          string                 `json:"user"`
	LogicTime     string                 `json:"logictime"`
	StartTime     string                 `json:"starttime"`
	EndTime       string                 `json:"endtime"`
	Status        string                 `json:"status"`
	DbStore       string                 `json:"dbstore"`
	ParaMap       map[string]interface{} `json:"paramap"`
	Elapsed       string                 `json:"elapsed"`
	KeepPeriod    string                 `json:"keepPeriod"`
	Ip            string                 `json:"ip"`
	Port          string                 `json:"port"`
        HomeDir       string                 `json:"homedir"`
	Enable        string                 `json:"enable"`
}
