package module

type MetaJobPoolBean struct {
	FlowId     string                 `json:"flowid"`
	Sys        string                 `json:"sys"`
	Job        string                 `json:"job"`
	StartTime  string                 `json:"starttime"`
        Priority   string                 `json:"priority"`
        Server     string                 `json:"server"`
	Enable     string                 `json:"enable"`
}
