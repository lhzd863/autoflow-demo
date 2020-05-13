package module

type MetaJobSlaveHeartBean struct {
	SlaveId    string `json:"slaveid"`
	Ip         string `json:"ip"`
	Port       string `json:"port"`
	UpdateTime string `json:"updatetime"`
	MaxCnt     string `json:"maxcnt"`
        RunningCnt string `json:"runningcnt"`
        CurrentCnt string `json:"currentcnt"`
	Enable     string `json:"enable"`
}
