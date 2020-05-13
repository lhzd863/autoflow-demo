package module

type MetaJobBean struct {
	Sys            string `json:"sys"`
	Job            string `json:"job"`
	Enable         string `json:"enable"`
	Server         string `json:"server"`
        Ip             string `json:"ip"`
        Port           string `json:"port"`
	TimeWindow     string `json:"timewindow"`
	RetryCnt       string `json:"retrycnt"`
	Alert          string `json:"alert"`
	TimeTrigger    string `json:"timetrigger"`
	JobType        string `json:"jobtype"`
	Frequency      string `json:"frequency"`
	Status         string `json:"status"`
	StartTime      string `json:"starttime"`
        EndTime        string `json:"endtime"`
	RunTime        string `json:"runtime"`
	CheckBatStatus string `json:"checkbatstatus"`
	Priority       string `json:"priority"`
	RunningCmd     string `json:"runningcmd"`
}
