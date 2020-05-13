package module

type RetBean struct {
	Status_Txt  string      `json:"status_txt"`
	Status_Code int32       `json:"status_code"`
	Code        string      `json:"code"`
	Status      string      `json:"status"`
	Err         string      `json:"err"`
	Data        interface{} `json:"data"`
}
