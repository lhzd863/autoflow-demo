package module

type MetaJobWorkerBean struct {
        Id          string        `json:"id"`
	FlowId      string        `json:"flowid"`
	Sys         string        `json:"sys"`
	Job         string        `json:"job"`
	RetryCnt    string        `json:"retrycnt"`
	Alert       string        `json:"alert"`
	Status      string        `json:"status"`
	StartTime   string        `json:"stt"`
	EndTime     string        `json:"edt"`
	RunningTime string        `json:"rt"`
	RunningCmd  []interface{} `json:"rc"`
	Parameter   []interface{} `json:"para"`
	MasterIp    string        `json:"mip"`
	MasterPort  string        `json:"mport"`
	SlaveIp     string        `json:"sip"`
	SlavePort   string        `json:"sport"`
}
