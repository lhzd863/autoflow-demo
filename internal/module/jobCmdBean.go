package module

type MetaJobCmdBean struct {
	Sys        string `json:"sys"`
	Job        string `json:"job"`
	Type       string `json:"type"`
	Step       string `json:"step"`
	Cmd        string `json:"cmd"`
	Parameters string `json:"parameters"`
	Enable     string `json:"enable"`
}

