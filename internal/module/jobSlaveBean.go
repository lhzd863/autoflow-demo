package module

type MetaJobSlaveBean struct {
	SlaveId    string `json:"slaveid"`
	Ip         string `json:"ip"`
	Port       string `json:"port"`
	UpdateTime string `json:"updatetime"`
	Cnt        string `json:"cnt"`
	Enable     string `json:"enable"`
}
