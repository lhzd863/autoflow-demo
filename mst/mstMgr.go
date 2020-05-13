package mst

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
        "runtime"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
  
        "github.com/satori/go.uuid"

	"github.com/lhzd863/autoflow/internal/glog"
	"github.com/lhzd863/autoflow/internal/gproto"
	"github.com/lhzd863/autoflow/internal/module"
	"github.com/lhzd863/autoflow/internal/util"
)

type MstMgr struct {
	MstId         string
	FlowId        string
	ApiServerIp   string
	ApiServerPort string
	Name          string
	Timestamp     int64
	Attempts      uint16
	StopFlag      bool
	MaxJobCount   int64
	QueueDir      string
	SleepTime     int64
	LogF          string
	HomeDir       string
	MstIp         string
	MstPort       string
	AccessToken   string
}

func NewMstMgr(flowid string, apiserverip string, apiserverport string, mstid string, homeDir string, mstip string, mstport string, accesstoken string) *MstMgr {
	return &MstMgr{
		MstId:         mstid,
		FlowId:        flowid,
		ApiServerIp:   apiserverip,
		ApiServerPort: apiserverport,
		Name:          "mst",
		Timestamp:     time.Now().Unix(),
		Attempts:      0,
		StopFlag:      false,
		QueueDir:      homeDir + "/queue",
		SleepTime:     10,
		MaxJobCount:   100,
		LogF:          homeDir + "/" + flowid + "/LOG/mst_" + mstid + "_${" + util.ENV_VAR_DATE + "}.log",
		HomeDir:       homeDir,
		MstIp:         mstip,
		MstPort:       mstport,
		AccessToken:   accesstoken,
	}
}

func (m *MstMgr) MainMgr() {
        var waitGroup util.WaitGroupWrapper
	for {
		glog.Glog(m.LogF, "mst "+m.MstId+" working...")
		if m.StopFlag {
			glog.Glog(m.LogF, "Exit mst main.")
			break
		}
                runtime.GOMAXPROCS(runtime.NumCPU())
                waitGroup.Wrap(func() { m.checkPending()})
		waitGroup.Wrap(func() { m.checkGo()})
                waitGroup.Wait()
		time.Sleep(time.Duration(m.SleepTime) * time.Second)
	}
}

//check status go
func (m *MstMgr) checkGo() bool {
	glog.Glog(m.LogF, "Checking status Pending.")
	url := fmt.Sprintf("http://%v:%v/mst/job/status/go?accesstoken=%v&mstid=%v", m.MstIp, m.MstPort, m.AccessToken, m.MstId)
	jsonstr, err := util.Api_RequestPost(url, "{}")
	if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(m.LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
		return false
	}
	retbn := new(module.RetBean)
	err = json.Unmarshal([]byte(jsonstr), &retbn)
	if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
		return false
	}
	if retbn.Status_Code != 200 {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(LogF, fmt.Sprintf("%v %v post url return status code:%v",cfile, cline, retbn.Status_Code))
		return false
	}
	if retbn.Data == nil {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(LogF, fmt.Sprintf("%v %v get pending status job err.",cfile, cline))
		return false
	}
	retarr := (retbn.Data).([]interface{})
	for i := 0; i < len(retarr); i++ {
		v := retarr[i].(map[string]interface{})
		if v["enable"] != "1" {
			glog.Glog(m.LogF, fmt.Sprintf("Job %v %v is not enabled,wait for next time.", v["job"], v["enable"]))
			continue
		}
		//execute job
                
                var waitGroup util.WaitGroupWrapper
                jobv := m.jobInfo(v["sys"].(string),v["job"].(string))
		if v["jobtype"].(string) == "V" {
                         //var waitGroup util.WaitGroupWrapper
                         waitGroup.Wrap(func() { m.invokeVirtualJob(jobv[0].(map[string]interface {}))})
		} else {
                         //var waitGroup util.WaitGroupWrapper
                         waitGroup.Wrap(func() { m.invokeRealJob(jobv[0].(map[string]interface {}))})
		}
	}
	if len(retarr) == 0 {
		glog.Glog(m.LogF, fmt.Sprint("no go job."))
	}
	return true
}

func (m *MstMgr) invokeVirtualJob(job map[string]interface{})  {
	glog.Glog(m.LogF, fmt.Sprintf("Virtual %v %v", job["sys"], job["job"]))
	//change job status
        u1 := uuid.Must(uuid.NewV4())
	retes := m.jobStatusUpdate(job,util.SYS_STATUS_DONE,"et",fmt.Sprint(u1))
	//stream job
	if retes {
		_ = m.streamJob(job)
	}
}

func (m *MstMgr) jobStatusUpdate(job map[string]interface{},status string,code string,id string) bool {
        url := fmt.Sprintf("http://%v:%v/mst/job/status/update?accesstoken=%v&sys=%v&job=%v&status=%v&code=%v&id=%v", m.MstIp, m.MstPort, m.AccessToken, job["sys"].(string), job["job"].(string), status,code,id)
        jsonstr, err := util.Api_RequestPost(url, "{}")
        if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
                glog.Glog(m.LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
                return false
        }
        retbn := new(module.RetBean)
        err = json.Unmarshal([]byte(jsonstr), &retbn)
        if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
                glog.Glog(LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
                return false
        }
        if retbn.Status_Code != 200 {
                glog.Glog(LogF, fmt.Sprintf("post url return status code:%v", retbn.Status_Code))
                return false
        }
	return true
}

func (m *MstMgr) jobStepCmd(sys string,job string) []interface{} {
        retarr := make([]interface{}, 0)
        url := fmt.Sprintf("http://%v:%v/mst/job/cmd/ls?accesstoken=%v&sys=%v&job=%v",m.MstIp,m.MstPort,m.AccessToken,sys,job)
        glog.Glog(LogF, fmt.Sprintf("%v",url))
        jsonstr, err := util.Api_RequestPost(url, "{}")
        if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
                glog.Glog(m.LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
                return retarr
        }
        retbn := new(module.RetBean)
        err = json.Unmarshal([]byte(jsonstr), &retbn)
        if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
                glog.Glog(LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
                return retarr
        }
        if retbn.Status_Code != 200 {
                glog.Glog(LogF, fmt.Sprintf("post url return status code:%v", retbn.Status_Code))
                return retarr
        }
        if retbn.Data == nil {
                glog.Glog(LogF, fmt.Sprintf("get pending status job err."))
                return retarr
        }
        return (retbn.Data).([]interface{})
}

func (m *MstMgr) streamJob(job map[string]interface{}) bool {
	glog.Glog(m.LogF, fmt.Sprintf("stream %v %v", job["sys"], job["job"]))
	return true
}

func (m *MstMgr) invokeRealJob(job map[string]interface{}) {
	glog.Glog(m.LogF, fmt.Sprintf("exec %v,%v on slave %v [%v:%v].", job["sys"], job["job"], job["slaveid"], job["ip"], job["port"]))
	// 建立连接到gRPC服务
	conn, err := grpc.Dial(job["ip"].(string)+":"+job["port"].(string), grpc.WithInsecure())
	if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(m.LogF, fmt.Sprintf("%v %v did not connect: %v",cfile, cline, err))
		return 
	}
	// 函数结束时关闭连接
	defer conn.Close()

	// 创建Waiter服务的客户端
	t := gproto.NewSlaverClient(conn)

	mjwb := new(module.MetaJobWorkerBean)
        u1 := uuid.Must(uuid.NewV4())
        mjwb.Id = fmt.Sprint(u1)
	mjwb.FlowId = FlowId
	mjwb.Sys = job["sys"].(string)
	mjwb.Job = job["job"].(string)
	mjwb.RetryCnt = job["retrycnt"].(string)
	mjwb.Alert = job["alert"].(string)
	mjwb.Status = util.SYS_STATUS_RUNNING
	timeStr := time.Now().Format("2006-01-02 15:04:05")
	mjwb.StartTime = timeStr
	mjwb.MasterIp = m.MstIp
	mjwb.MasterPort = m.MstPort
	mjwb.SlaveIp = job["ip"].(string)
	mjwb.SlavePort = job["port"].(string)
        glog.Glog(m.LogF, fmt.Sprintf("%v","start"))
        arrstep := m.jobStepCmd(job["sys"].(string),job["job"].(string))
        glog.Glog(m.LogF, fmt.Sprintf("%v","end"))
        mjwb.RunningCmd = make([]interface{},0)
        for i:=0;i<len(arrstep);i++ {
             ast := arrstep[i].(map[string]interface{})
             mjwb.RunningCmd = append(mjwb.RunningCmd,ast["cmd"].(string))
        }
	jsonstr, _ := json.Marshal(mjwb)
	if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(m.LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
		return
	}
	// 调用gRPC接口
        job["endtime"] = timeStr
        glog.Glog(m.LogF, fmt.Sprint(job))
        m.jobStatusUpdate(job,util.SYS_STATUS_RUNNING,"st",fmt.Sprint(u1))
        
        retes := false
	tr, err := t.JobStart(context.Background(), &gproto.Req{JsonStr: string(jsonstr)})
	if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(m.LogF, fmt.Sprintf("%v %v could not greet: %v",cfile, cline ,err))
                retes = m.jobStatusUpdate(job,util.SYS_STATUS_FAIL,"et",fmt.Sprint(u1))
		return
	}
	//change job status
	if tr.Status_Code == 200 {
                glog.Glog(m.LogF, fmt.Sprint(job))
		retes = m.jobStatusUpdate(job,util.SYS_STATUS_DONE,"et",fmt.Sprint(u1))
	} else {
		glog.Glog(LogF, fmt.Sprint(tr.Status_Txt))
                retes = m.jobStatusUpdate(job,util.SYS_STATUS_FAIL,"et",fmt.Sprint(u1))
		return
	}
	//stream job
	if retes {
		_ = m.streamJob(job)
	}
}

func (m *MstMgr) jobInfo(sys string,job string) []interface{} {
        retarr := make([]interface{}, 0)
        url := fmt.Sprintf("http://%v:%v/mst/job/get?accesstoken=%v&sys=%v&job=%v", m.MstIp, m.MstPort, m.AccessToken,sys,job)
        jsonstr, err := util.Api_RequestPost(url, "{}")
        if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
                glog.Glog(m.LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
                return retarr
        }
        retbn := new(module.RetBean)
        err = json.Unmarshal([]byte(jsonstr), &retbn)
        if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
                glog.Glog(LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
                return retarr
        }
        if retbn.Status_Code != 200 {
                glog.Glog(LogF, fmt.Sprintf("post url return status code:%v", retbn.Status_Code))
                return retarr
        }
        if retbn.Data == nil {
                glog.Glog(LogF, fmt.Sprintf("get pending status job err."))
                return retarr
        }
	return (retbn.Data).([]interface{})
}

//Check the status pending
func (m *MstMgr) checkPending() bool {
	glog.Glog(m.LogF, "Checking status Pending.")
	url := fmt.Sprintf("http://%v:%v/mst/job/status/pending?accesstoken=%v&mstid=%v", m.MstIp, m.MstPort, m.AccessToken, m.MstId)
	jsonstr, err := util.Api_RequestPost(url, "{}")
        glog.Glog(m.LogF, fmt.Sprint(string(jsonstr)))
	if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(m.LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
		return false
	}
	retbn := new(module.RetBean)
	err = json.Unmarshal([]byte(jsonstr), &retbn)
	if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
		return false
	}
	if retbn.Status_Code != 200 {
		glog.Glog(LogF, fmt.Sprintf("post url return status code:%v", retbn.Status_Code))
		return false
	}
	if retbn.Data == nil {
		glog.Glog(LogF, fmt.Sprint("get pending status job err."))
		return false
	}
	retarr := (retbn.Data).([]interface{})
	for i := 0; i < len(retarr); i++ {
		v := retarr[i].(map[string]interface{})
		if v["enable"] != "1" {
			glog.Glog(m.LogF, fmt.Sprintf("Job %v %v is not enabled,wait for next time.", v["job"], v["enable"]))
			continue
		}
		glog.Glog(m.LogF, "Check Job Dependency.")
		if !m.isDependantOk(v) {
			continue
		}
		glog.Glog(m.LogF, "Check Job Time Window.")
		if !m.isTimeWindowOk(v) {
			continue
		}
		glog.Glog(m.LogF, "Check Job Cmd.")
		if !m.isCmdOk(v) {
			continue
		}
		m.submitJob(v)
		glog.Glog(m.LogF, fmt.Sprint(v["job"]))
		//}
	}
	if len(retarr) == 0 {
		glog.Glog(m.LogF, fmt.Sprint("no pending job."))
	}
	return true
}

//Check the job queue in repository and try to get the job
func (m *MstMgr) submitJob(job map[string]interface{}) bool {
	glog.Glog(m.LogF, fmt.Sprintf("Invoke job for %v %v", job["sys"], job["job"]))
	url0 := fmt.Sprintf("http://%v:%v/api/v1/job/pool/add?accesstoken=%v", m.ApiServerIp, m.ApiServerPort, m.AccessToken)
	jpb := fmt.Sprintf("{\"sys\":\"%v\",\"job\":\"%v\",\"flowid\":\"%v\",\"priority\":\"%v\"}", job["sys"], job["job"], m.FlowId, job["priority"])
	jsonstr1, err := util.Api_RequestPost(url0, jpb)
	if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(m.LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
		return false
	}
	retbn1 := new(module.RetBean)
	err = json.Unmarshal([]byte(jsonstr1), &retbn1)
	if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
		return false
	}
	if retbn1.Status_Code != 200 {
		glog.Glog(LogF, fmt.Sprintf("post url return status code:%v", retbn1.Status_Code))
		return false
	}

	url := fmt.Sprintf("http://%v:%v/mst/job/status/update?accesstoken=%v&sys=%v&job=%v&status=%v&code=no", m.MstIp, m.MstPort, m.AccessToken, job["sys"], job["job"], util.SYS_STATUS_SUBMIT)
	jsonstr, err := util.Api_RequestPost(url, "{}")
	if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(m.LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
		return false
	}
	retbn := new(module.RetBean)
	err = json.Unmarshal([]byte(jsonstr), &retbn)
	if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
		return false
	}
	if retbn.Status_Code != 200 {
		glog.Glog(LogF, fmt.Sprintf("post url return status code:%v", retbn.Status_Code))
		return false
	}
	return true
}

func (m *MstMgr) processControlFile(ctlinfo interface{}) bool {
	return true
}

//Sort the control file by prior
func (m *MstMgr) sortControlFile(fmap map[string]interface{}) []interface{} {
	var i = 0
	alen := len(fmap)
	var arr = make([]interface{}, alen)
	for _, v := range fmap {
		arr[i] = v
		i++
	}
	return arr
}

func (m *MstMgr) getCurrentJobCount() int64 {
	return 1
}

func (m *MstMgr) isControlFile(filename string) bool {
	arr := strings.Split(filename, ".")
	if len(arr) == 3 {
		return true
	} else {
		return false
	}
}

func (m *MstMgr) mstCtlMarshal() ([]byte, error) {
	ctl := new(MetaJobCTL)
	data, err := json.Marshal(ctl)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (m *MstMgr) mstCtlUnmarshal(data []byte) (MetaJobCTL, error) {
	var ctl MetaJobCTL
	err := json.Unmarshal(data, &ctl)
	if err != nil {
		return ctl, err
	}
	return ctl, nil
}

func (m *MstMgr) mstCtlRead(f string) ([]byte, error) {
	fp, err := os.OpenFile(f, os.O_RDONLY, 0755)
	defer fp.Close()
	if err != nil {
		return nil, err
	}
	data := make([]byte, 1024)
	n, err := fp.Read(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *MstMgr) mstCtlWrite(f string, data []byte) error {
	fp, err := os.OpenFile(f, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer fp.Close()
	_, err = fp.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func (m *MstMgr) isDependantOk(job map[string]interface{}) bool {
	url := fmt.Sprintf("http://%v:%v/mst/job/dependency?accesstoken=%v&sys=%v&job=%v", m.MstIp, m.MstPort, m.AccessToken, job["sys"], job["job"])
	jsonstr, err := util.Api_RequestPost(url, "{}")
	if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(m.LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
		return false
	}
	retbn := new(module.RetBean)
	err = json.Unmarshal([]byte(jsonstr), &retbn)
	if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
		return false
	}
	if retbn.Status_Code != 200 {
		glog.Glog(LogF, fmt.Sprintf("status code:%v", retbn.Status_Code))
		return false
	}
	if retbn.Data == nil {
		return false
	}
	retarr := (retbn.Data).([]interface{})
	if len(retarr) > 0 {
		v := retarr[0].(map[string]interface{})
		glog.Glog(m.LogF, fmt.Sprintf("There is dependant job %v %v running, wait for next time!", v["sys"], v["job"]))
		return false
	}
	return true
}

func (m *MstMgr) isTimeWindowOk(job map[string]interface{}) bool {
	url := fmt.Sprintf("http://%v:%v/mst/job/timewindow?accesstoken=%v&sys=%v&job=%v", m.MstIp, m.MstPort, m.AccessToken, job["sys"], job["job"])
	jsonstr, err := util.Api_RequestPost(url, "{}")
	if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(m.LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
		return false
	}
	retbn := new(module.RetBean)
	err = json.Unmarshal([]byte(jsonstr), &retbn)
	if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
		return false
	}
	if retbn.Status_Code != 200 {
		glog.Glog(LogF, fmt.Sprintf("status code:%v", retbn.Status_Code))
		return false
	}
	if retbn.Data == nil {
		return false
	}
	retarr := (retbn.Data).([]interface{})
	if len(retarr) > 0 {
		v := retarr[0].(map[string]interface{})
		glog.Glog(m.LogF, fmt.Sprintf("The current hour does not match the job %v %v time window, wait for next time.", v["sys"], v["job"]))
                
		return false
	}
	return true
}

func (m *MstMgr) isCmdOk(job map[string]interface{}) bool {
	url := fmt.Sprintf("http://%v:%v/mst/job/cmd/ls?accesstoken=%v&sys=%v&job=%v", m.MstIp, m.MstPort, m.AccessToken, job["sys"], job["job"])
	jsonstr, err := util.Api_RequestPost(url, "{}")
	if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(m.LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
		return false
	}
	retbn := new(module.RetBean)
	err = json.Unmarshal([]byte(jsonstr), &retbn)
	if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
		return false
	}
	if retbn.Status_Code != 200 {
		glog.Glog(LogF, fmt.Sprintf("status code:%v", retbn.Status_Code))
		return false
	}
	if retbn.Data == nil {
		return false
	}
	retarr := (retbn.Data).([]interface{})
	if len(retarr) > 0 {
                return true
	}
        glog.Glog(m.LogF, fmt.Sprintf("The job %v, %v is not exists cmd, wait for next time.", job["sys"], job["job"]))
	return false
}


