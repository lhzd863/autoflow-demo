package mst

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
        "runtime"
        "io/ioutil"

        "golang.org/x/net/context"
        "google.golang.org/grpc"

	"github.com/lhzd863/autoflow/internal/db"
	"github.com/lhzd863/autoflow/internal/glog"
	"github.com/lhzd863/autoflow/internal/jwt"
	"github.com/lhzd863/autoflow/internal/module"
	"github.com/lhzd863/autoflow/internal/util"
	"github.com/lhzd863/autoflow/internal/workpool"
        "github.com/lhzd863/autoflow/internal/gproto"
)

var (
	FlowId        string
	ApiServerIp   string
	ApiServerPort string
	LogF          string
	Msgch         chan string
	CHashRing     *Consistent
	HomeDir       string
	AccessToken   string
	ProcessNum    int
	jobpool       = db.NewMemDB()
        workerpool    = db.NewMemDB()
        MstIp        string
        MstPort      string
)

type Mst struct {
	JwtKey       string
	FlowId       string
	waitGroup    util.WaitGroupWrapper
	HttpListener net.Listener
}

func NewMst(paraMap map[string]interface{}, httpListener net.Listener) *Mst {
	FlowId = paraMap["flowid"].(string)
	ApiServerIp = paraMap["apiserverip"].(string)
	ApiServerPort = paraMap["apiserverport"].(string)
	HomeDir = paraMap["homedir"].(string)
	AccessToken = paraMap["accesstoken"].(string)
	NumStr := paraMap["processnum"].(string)
	ProcessNum, _ = strconv.Atoi(NumStr)
	LogF = HomeDir + "/" + FlowId + "/mgr_${" + util.ENV_VAR_DATE + "}.log"
        MstIp = paraMap["ip"].(string)
        MstPort = paraMap["port"].(string)
	m := &Mst{
		JwtKey:       paraMap["jwtkey"].(string),
		FlowId:       FlowId,
		HttpListener: httpListener,
	}
	return m
}

func (m *Mst) Main() {
	if ok, _ := util.PathExists(HomeDir + "/" + FlowId + "/LOG"); !ok {
		os.Mkdir(HomeDir+"/"+FlowId+"/LOG", os.ModePerm)
	}
	glog.Glog(LogF, fmt.Sprintf("HTTP: listening on %s", m.HttpListener.Addr().String()))
	m.waitGroup.Wrap(func() { m.HttpServer(m.HttpListener) })
	Msgch = make(chan string)
	CHashRing = NewConsistent()
	glog.Glog(LogF, fmt.Sprintf("process number:%v", ProcessNum))
	for i := 0; i < ProcessNum; i++ {
		si := fmt.Sprintf("%d", i)
		CHashRing.Add(NewNode(i, si, 1))
	}
	workPool := workpool.New(ProcessNum, 1000)
	for j := 0; j < ProcessNum; j++ {
		mw := new(MetaMyWork)
		mw.HomeDir = HomeDir
		mw.FlowId = FlowId
		mw.ApiServerIp = ApiServerIp
		mw.ApiServerPort = ApiServerPort
		mw.MstIp = MstIp
		mw.MstPort = MstPort
		mw.AccessToken = AccessToken
		work := &MyWork{
			Id:   fmt.Sprint(j),
			Name: "A-" + fmt.Sprint(j),
			WP:   workPool,
			Mmw:  mw,
		}
		err := workPool.PostWork(fmt.Sprintf("name_routine_%v",j), work)
		if err != nil {
			glog.Glog(LogF, fmt.Sprintf("ERROR: %s\n", err))
			continue
		}
                mwpm:= new(module.MetaWorkerPoolMemBean)
                mwpm.Id = fmt.Sprintf("name_routine_%v",j)
                mwpm.WorkerPool = workPool
                workerpool.Add(mwpm.Id,mwpm)
	}
}
func (m *Mst) CloseServer() {
        j :=0
        for k := range workerpool.MemMap {
                mwpm := (workerpool.MemMap[k]).(*module.MetaWorkerPoolMemBean)
                mwpm.WorkerPool.Shutdown(fmt.Sprintf("name_routine_%v",j))
                workerpool.Remove(fmt.Sprintf("name_routine_%v",j))
                j++
        }
        //m.HttpListener.Close()
}

func (m *Mst) HttpServer(listener net.Listener) {
	handler := http.NewServeMux()
	handler.HandleFunc("/ping", pingHandler)
	handler.HandleFunc("/mst/jobqueue/add", JobQueueAddHandler)
	handler.HandleFunc("/mst/jobqueue/get", JobQueueGetHandler)
	handler.HandleFunc("/mst/jobqueue/ls", JobQueueListHandler)
	handler.HandleFunc("/mst/jobqueue/rm", JobQueueRemoveHandler)
	handler.HandleFunc("/mst/job/add", JobAddHandler)
	handler.HandleFunc("/mst/job/get", JobGetHandler)
	handler.HandleFunc("/mst/job/rm", JobRemoveHandler)
	handler.HandleFunc("/mst/job/ls", JobListHandler)
	handler.HandleFunc("/mst/job/status/pending", JobStatusPendingHandler)
	handler.HandleFunc("/mst/job/status/go", JobStatusGoHandler)
	handler.HandleFunc("/mst/job/status/update", JobStatusUpdateHandler)
	handler.HandleFunc("/mst/job/status/update/go", JobUpdateStatusGoHandler)
	handler.HandleFunc("/mst/job/server/update", JobUpdateServerHandler)
	handler.HandleFunc("/mst/job/dependency", JobDependencyHandler)
	handler.HandleFunc("/mst/job/timewindow", JobTimeWindowHandler)
	handler.HandleFunc("/mst/job/timewindow/add", JobTimeWindowAddHandler)
	handler.HandleFunc("/mst/job/timewindow/get", JobTimeWindowGetHandler)
	handler.HandleFunc("/mst/job/timewindow/rm", JobTimeWindowRemoveHandler)
	handler.HandleFunc("/mst/job/cmd/ls", JobCmdListHandler)
	handler.HandleFunc("/mst/job/cmd/add", JobCmdAddHandler)
	handler.HandleFunc("/mst/job/cmd/get", JobCmdGetHandler)
	handler.HandleFunc("/mst/job/cmd/rm", JobCmdRemoveHandler)
	handler.HandleFunc("/mst/pool/ls", JobPoolListHandler)
	handler.HandleFunc("/mst/pool/rm", JobPoolRemoveHandler)
        handler.HandleFunc("/mst/job/stop", JobStopHandler)
        handler.HandleFunc("/mst/job/update", JobUpdateHandler)

	// these timeouts are absolute per server connection NOT per request
	// this means that a single persistent connection will only last N seconds
	server := &http.Server{
		Handler: handler,
	}
	err := server.Serve(listener)
	// theres no direct way to detect this error because it is not exposed
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		glog.Glog(LogF, fmt.Sprintf("ERROR: http.Serve() - %s", err.Error()))
	}
	glog.Glog(LogF, fmt.Sprintf("HTTP: closing %s", listener.Addr().String()))
}

func pingHandler(response http.ResponseWriter, request *http.Request) {
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprint("Not Authorized.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Not Authorized.%v", err), nil)
		return
	}
	util.ApiResponse(response, 200, "", nil)
}

func JobQueueAddHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode.%v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}
	txts, err := reqParams.Get("txts")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get txts parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get txts parameter failed.%v", err), nil)
		return
	}

	queuebean := new(MetaJobQueue)
	queuebean.Sys = sys
	queuebean.Job = job
	queuebean.Txts = txts
	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_QUEUE)
	defer bt.Close()
	timeStr := time.Now().Format("2006-01-02 15:04:05")
	queuebean.Cts = timeStr
	queuebean.Enable = "1"
	jsonstr, _ := json.Marshal(queuebean)
	err = bt.Set(queuebean.Sys+"."+queuebean.Job, string(jsonstr))
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("data in db update error.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("data in db update error.%v", err), nil)
		return
	}
	util.ApiResponse(response, 200, "", nil)
}

func JobAddHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode.%v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}
	jobbean := new(module.MetaJobBean)
	jobbean.Sys = sys
	jobbean.Job = job
	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_CONF)
	defer bt.Close()
	timeStr := time.Now().Format("2006-01-02 15:04:05")
	jobbean.Enable = "1"
	jobbean.StartTime = timeStr
	jobbean.JobType = "D"
	jobbean.Frequency = "D"
	jobbean.Status = util.SYS_STATUS_READY
	jobbean.Priority = "0"
	jsonstr, _ := json.Marshal(jobbean)
	err = bt.Set(jobbean.Sys+"."+jobbean.Job, string(jsonstr))
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("data in db update error.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("data in db update error.%v", err), nil)
		return
	}
	util.ApiResponse(response, 200, "", nil)
}

func JobUpdateHandler(response http.ResponseWriter, request *http.Request) {
        //glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
        if request.Method != "POST" {
                glog.Glog(LogF, "706 method is not post")
                util.ApiResponse(response, 706, "method is not post", nil)
                return
        }
        err := globalOauth(response, request)
        if err != nil {
                glog.Glog(LogF, "700 Not Authorized")
                util.ApiResponse(response, 700, "Not Authorized", nil)
                return
        }
        body, err := ioutil.ReadAll(request.Body)
        if err != nil {
                glog.Glog(LogF, "Body parse error.")
                util.ApiResponse(response, 700, "Body parse error.", nil)
                return
        }
        defer request.Body.Close()

        jobbean := new(module.MetaJobBean)
        err = json.Unmarshal(body, jobbean)
        if err != nil {
                glog.Glog(LogF, "json parse error.")
                util.ApiResponse(response, 700, "json parse error.", nil)
                return
        }
        glog.Glog(LogF, fmt.Sprint(jobbean))
}

func JobGetHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode: %v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_CONF)
	defer bt.Close()

	retlst := make([]interface{}, 0)
	jobn := bt.Get(sys + "." + job)
	if jobn != nil {
		jobbn := new(module.MetaJobBean)
		err := json.Unmarshal([]byte(jobn.(string)), &jobbn)
		if err != nil {
			glog.Glog(LogF, fmt.Sprint(err))
		}
		retlst = append(retlst, jobbn)
	}
	util.ApiResponse(response, 200, "", retlst)
}

func JobRemoveHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode: %v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}

	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_CONF)
	defer bt.Close()

	err = bt.Remove(sys + "." + job)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("rm data err.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("rm data err.%v", err), nil)
		return
	}
	util.ApiResponse(response, 200, "", nil)
}

func JobUpdateStatusHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode.%v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}
	status, err := reqParams.Get("status")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get status parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get status parameter failed.%v", err), nil)
		return
	}
	jobbean := new(module.MetaJobBean)
	jobbean.Sys = sys
	jobbean.Job = job
	jobbean.Status = status
	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_CONF)
	defer bt.Close()

	jobbn := new(module.MetaJobBean)
	jobn := bt.Get(sys + "." + job)
	if jobn != nil {
		err := json.Unmarshal([]byte(jobn.(string)), &jobbn)
		if err != nil {
			glog.Glog(LogF, fmt.Sprint(err))
			util.ApiResponse(response, 700, fmt.Sprintf("%v", err), nil)
			return
		}
	}
	timeStr := time.Now().Format("2006-01-02 15:04:05")
	jobbn.EndTime = timeStr
	jobbn.Status = status
	jsonstr, _ := json.Marshal(jobbn)
	err = bt.Set(jobbn.Sys+"."+jobbn.Job, string(jsonstr))
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("data in db update error.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("data in db update error.%v", err), nil)
		return
	}
	util.ApiResponse(response, 200, "", nil)
}

func JobStatusUpdateHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode.%v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}
	status, err := reqParams.Get("status")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get status parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get status parameter failed.%v", err), nil)
		return
	}
	code, err := reqParams.Get("code")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get code parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get code parameter failed.%v", err), nil)
		return
	}

	id, err := reqParams.Get("id")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get id parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get id parameter failed.%v", err), nil)
		return
	}

	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_CONF)
	defer bt.Close()

	jobbn := new(module.MetaJobBean)
	jobn := bt.Get(sys + "." + job)
	if jobn != nil {
		err := json.Unmarshal([]byte(jobn.(string)), &jobbn)
		if err != nil {
			glog.Glog(LogF, fmt.Sprint(err))
			util.ApiResponse(response, 700, fmt.Sprintf("%v", err), nil)
			return
		}
	}
	timeStr := time.Now().Format("2006-01-02 15:04:05")
	if code == "st" {
		jobbn.StartTime = timeStr
		jobbn.EndTime = ""
	}
	if code == "et" {
		jobbn.EndTime = timeStr
	}
	jobbn.Status = status
	jsonstr, _ := json.Marshal(jobbn)
	err = bt.Set(jobbn.Sys+"."+jobbn.Job, string(jsonstr))
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("data in db update error.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("data in db update error.%v", err), nil)
		return
	}
	if code == "st" {
                mjm := new(module.MetaJobMemBean)
                mjm.Id = id
                mjm.Sys = jobbn.Sys
                mjm.Job = jobbn.Job
                mjm.CreateTime = timeStr
                mjm.Enable = "1"
		jobpool.Add(id, mjm)
	}
	if code == "et" {
		jobpool.Remove(id)
	}
	util.ApiResponse(response, 200, "", nil)
}

func JobUpdateStatusGoHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode.%v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}
	status, err := reqParams.Get("status")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get status parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get status parameter failed.%v", err), nil)
		return
	}
	ip, err := reqParams.Get("ip")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get ip parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get ip parameter failed.%v", err), nil)
		return
	}
	port, err := reqParams.Get("port")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get port parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get port parameter failed.%v", err), nil)
		return
	}
	server, err := reqParams.Get("server")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get server parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get server parameter failed.%v", err), nil)
		return
	}

	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_CONF)
	defer bt.Close()

	jobbn := new(module.MetaJobBean)
	jobn := bt.Get(sys + "." + job)
	if jobn != nil {
		err := json.Unmarshal([]byte(jobn.(string)), &jobbn)
		if err != nil {
			glog.Glog(LogF, fmt.Sprint(err))
			util.ApiResponse(response, 700, fmt.Sprintf("%v", err), nil)
			return
		}
	}

	jobbn.Status = status
	jobbn.Ip = ip
	jobbn.Port = port
	jobbn.Server = server
	jsonstr, _ := json.Marshal(jobbn)
	err = bt.Set(jobbn.Sys+"."+jobbn.Job, string(jsonstr))
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("data in db update error.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("data in db update error.%v", err), nil)
		return
	}
	util.ApiResponse(response, 200, "", nil)
}

func JobUpdateServerHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode.%v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}
	server, err := reqParams.Get("server")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get server parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get server parameter failed.%v", err), nil)
		return
	}
	status, err := reqParams.Get("status")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get status parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get status parameter failed.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_CONF)
	defer bt.Close()

	jobbn := new(module.MetaJobBean)
	jobn := bt.Get(sys + "." + job)
	if jobn != nil {
		err := json.Unmarshal([]byte(jobn.(string)), &jobbn)
		if err != nil {
			glog.Glog(LogF, fmt.Sprint(err))
			util.ApiResponse(response, 700, fmt.Sprintf("%v", err), nil)
			return
		}
	}

	jobbn.Status = status
	jobbn.Server = server
	jsonstr, _ := json.Marshal(jobbn)
	err = bt.Set(jobbn.Sys+"."+jobbn.Job, string(jsonstr))
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("data in db update error.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("data in db update error.%v", err), nil)
		return
	}
	util.ApiResponse(response, 200, "", nil)
}

func JobListHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	//if request.Method != "POST" {
	//	glog.Glog(LogF, "706 method is not post")
	//	util.ApiResponse(response, 706, "method is not post", nil)
	//	return
	//}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}

	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_CONF)
	defer bt.Close()

	strlist := bt.Scan()
	retlst := make([]interface{}, 0)
	for _, v := range strlist {
		for _, v1 := range v.(map[string]interface{}) {
			mjb := new(module.MetaJobBean)
			err := json.Unmarshal([]byte(v1.(string)), &mjb)
			if err != nil {
				glog.Glog(LogF, fmt.Sprint(err))
				continue
			}
			retlst = append(retlst, mjb)
		}
	}
	util.ApiResponse(response, 200, "", retlst)
}

func JobQueueGetHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode: %v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}

	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_QUEUE)
	defer bt.Close()

	retlst := make([]interface{}, 0)
	qb := bt.Get(sys + "." + job)
	if qb != nil {
		jqb := new(MetaJobQueue)
		err := json.Unmarshal([]byte(qb.(string)), &jqb)
		if err != nil {
			glog.Glog(LogF, fmt.Sprint(err))
		}
		retlst = append(retlst, jqb)
	}
	util.ApiResponse(response, 200, "", retlst)
}

func JobQueueRemoveHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode: %v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}

	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_QUEUE)
	defer bt.Close()

	err = bt.Remove(sys + "." + job)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("rm data err.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("rm data err.%v", err), nil)
		return
	}
	util.ApiResponse(response, 200, "", nil)
}

func JobQueueListHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}

	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_QUEUE)
	defer bt.Close()

	strlist := bt.Scan()
	retlst := make([]interface{}, 0)
	for _, v := range strlist {
		for _, v1 := range v.(map[string]interface{}) {
			jqb := new(MetaJobQueue)
			err := json.Unmarshal([]byte(v1.(string)), &jqb)
			if err != nil {
				glog.Glog(LogF, fmt.Sprint(err))
				continue
			}
			retlst = append(retlst, jqb)
		}
	}
	util.ApiResponse(response, 200, "", retlst)
}

// Global Filter
func globalOauth(response http.ResponseWriter, request *http.Request) error {
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		return err
	}
	tokenstring, err := reqParams.Get("accesstoken")
	if err != nil {
		glog.Glog(LogF, "no accesstoken")
		return errors.New("no accesstoken")
	}

	var claimsDecoded map[string]interface{}
	decodeErr := jwt.Decode([]byte(tokenstring), &claimsDecoded, []byte("azz"))
	if decodeErr != nil {
		glog.Glog(LogF, "401: Not Authorized")
		return errors.New("401: Not Authorized")
	}

	exp := claimsDecoded["exp"].(float64)
	exp1, _ := strconv.ParseFloat(fmt.Sprintf("%v", time.Now().Unix()+0), 64)

	if (exp - exp1) < 0 {
		glog.Glog(LogF, fmt.Sprintf("401: Not Authorized AccessToken Expired %v %v %v ,Please login", exp, exp1, (exp-exp1)))
		return errors.New(fmt.Sprintf("401: Not Authorized AccessToken Expired %v %v %v ,Please login", exp, exp1, (exp - exp1)))
	}
	return nil
}

type MyWork struct {
	Id   string "id"
	Name string "The Name of a process"
	WP   *workpool.WorkPool
	Mmw  *MetaMyWork
}

func (workPool *MyWork) DoWork(workRoutine int) {
	glog.Glog(LogF, fmt.Sprintf("%s", workPool.Name))
	glog.Glog(LogF, fmt.Sprintf("*******> WR: %d  QW: %d  AR: %d", workRoutine, workPool.WP.QueuedWork(), workPool.WP.ActiveRoutines()))

	nmq := NewMstMgr(workPool.Mmw.FlowId, workPool.Mmw.ApiServerIp, workPool.Mmw.ApiServerPort, fmt.Sprintf("%v", workRoutine), workPool.Mmw.HomeDir, workPool.Mmw.MstIp, workPool.Mmw.MstPort, workPool.Mmw.AccessToken)
	nmq.MainMgr()
}

func JobStatusPendingHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode: %v", err), nil)
		return
	}
	mstid, err := reqParams.Get("mstid")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get mstid parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get mstid parameter failed.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_CONF)
	defer bt.Close()

	strlist := bt.Scan()
	retlst := make([]interface{}, 0)
	for _, v := range strlist {
		for _, v1 := range v.(map[string]interface{}) {
			jobbn := new(module.MetaJobBean)
			err := json.Unmarshal([]byte(v1.(string)), &jobbn)
			if err != nil {
				glog.Glog(LogF, fmt.Sprint(err))
				continue
			}
			if jobbn.Status != util.SYS_STATUS_PENDING {
				continue
			}
			jobnodeid := fmt.Sprintf("%v", CHashRing.Get(jobbn.Job).Id)
			if jobnodeid != mstid {
				glog.Glog(LogF, fmt.Sprintf("local node id %v ,job %v  mapping node id %v is not local node.", mstid, jobbn.Job, jobnodeid))
				continue
			}
			retlst = append(retlst, jobbn)
		}
	}
	util.ApiResponse(response, 200, "", retlst)
}

func JobStatusGoHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode: %v", err), nil)
		return
	}
	mstid, err := reqParams.Get("mstid")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get mstid parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get mstid parameter failed.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_CONF)
	defer bt.Close()

	strlist := bt.Scan()
	retlst := make([]interface{}, 0)
	for _, v := range strlist {
		for _, v1 := range v.(map[string]interface{}) {
			jobbn := new(module.MetaJobBean)
			err := json.Unmarshal([]byte(v1.(string)), &jobbn)
			if err != nil {
				glog.Glog(LogF, fmt.Sprint(err))
				continue
			}
			if jobbn.Status != util.SYS_STATUS_GO {
				continue
			}
			jobnodeid := fmt.Sprintf("%v", CHashRing.Get(jobbn.Job).Id)
			if jobnodeid != mstid {
				continue
			}
			retlst = append(retlst, jobbn)
		}
	}
	util.ApiResponse(response, 200, "", retlst)
}

func JobDependencyHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode: %v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_DEPENDENCY)
	defer bt.Close()

	strlist := bt.Scan()
	bt.Close()
	bt = db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_CONF)
	defer bt.Close()
	retlst := make([]interface{}, 0)
	for _, v := range strlist {
		for _, v1 := range v.(map[string]interface{}) {
			jobdb := new(module.MetaJobDependencyBean)
			err := json.Unmarshal([]byte(v1.(string)), &jobdb)
			if err != nil {
				glog.Glog(LogF, fmt.Sprint(err))
				continue
			}
			if jobdb.Enable != "1" {
				continue
			}
			if jobdb.Sys == sys && jobdb.Job == job {
				jobn := bt.Get(jobdb.DependencySys + "." + jobdb.DependencyJob)
				if jobn != nil {
					jobbn := new(module.MetaJobBean)
					err := json.Unmarshal([]byte(jobn.(string)), &jobbn)
					if err != nil {
						glog.Glog(LogF, fmt.Sprint(err))
					}
					if jobbn.Status != "Succ" || jobbn.Enable != "1" {
						continue
					}
					retlst = append(retlst, jobdb)
				}
			}
		}
	}
	util.ApiResponse(response, 200, "", retlst)
}

func JobTimeWindowHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode: %v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_TIMEWINDOW)
	defer bt.Close()

	//hourStr := time.Now().Format("15")
	//chour, err := strconv.Atoi(hourStr)
	//if err != nil {
	//	glog.Glog(LogF, fmt.Sprintf("iconv hour failed.%v", err))
	//	util.ApiResponse(response, 700, fmt.Sprintf("iconv hour failed.%v", err), nil)
	//	return
	//}

	retlst := make([]interface{}, 0)
	jobtw := bt.Get(sys + "." + job)
	if jobtw != nil {
		jtw := new(module.MetaJobTimeWindowBean)
		err := json.Unmarshal([]byte(jobtw.(string)), &jtw)
		if err != nil {
			glog.Glog(LogF, fmt.Sprint(err))
		}
		if jtw.Enable != "1" {
			retlst = append(retlst, jtw)
			util.ApiResponse(response, 200, "", retlst)
			return
		}
		bhour := jtw.StartHour
		ehour := jtw.EndHour
		if bhour < 0 || bhour > 23 || ehour < 0 || ehour > 23 {
			retlst = append(retlst, jtw)
			util.ApiResponse(response, 200, "", retlst)
			return
		}
	}

	util.ApiResponse(response, 200, "", retlst)
}

func JobCmdListHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode: %v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_CMD)
	defer bt.Close()
	retlst := make([]interface{}, 0)
	jobcmd := bt.Get(sys + "." + job)
	if jobcmd != nil {
		jcmd := new(module.MetaJobCmdBean)
		err := json.Unmarshal([]byte(jobcmd.(string)), &jcmd)
		if err != nil {
			glog.Glog(LogF, fmt.Sprint(err))
			util.ApiResponse(response, 700, fmt.Sprintf("get cmd failed.%v", err), nil)
			return
		}
		if jcmd.Enable != "1" {
			glog.Glog(LogF, fmt.Sprintf("%v %v step enable!=1.", sys, job))
			util.ApiResponse(response, 200, "", retlst)
			return
		}
		retlst = append(retlst, jcmd)
	}
	util.ApiResponse(response, 200, "", retlst)
}

func JobPoolListHandler(response http.ResponseWriter, request *http.Request) {
	retlst := make([]interface{}, 0)
	for k := range jobpool.MemMap {
		mjb := (jobpool.MemMap[k]).(*module.MetaJobMemBean)
		retlst = append(retlst, mjb)
	}
	util.ApiResponse(response, 200, "", retlst)
}

func JobPoolRemoveHandler(response http.ResponseWriter, request *http.Request) {
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode: %v", err), nil)
		return
	}
	id, err := reqParams.Get("id")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	retlst := make([]interface{}, 0)
	jobpool.Remove(id)
	util.ApiResponse(response, 200, "", retlst)
}

func JobStopHandler(response http.ResponseWriter, request *http.Request) {
        if request.Method != "POST" {
                glog.Glog(LogF, "706 method is not post")
                util.ApiResponse(response, 706, "method is not post", nil)
                return
        }
        err := globalOauth(response, request)
        if err != nil {
                glog.Glog(LogF, "700 Not Authorized")
                util.ApiResponse(response, 700, "Not Authorized", nil)
                return
        }
        reqParams, err := util.NewReqParams(request)
        if err != nil {
                glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
                util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode: %v", err), nil)
                return
        }
        id, err := reqParams.Get("id")
        if err != nil {
                glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
                util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
                return
        }
        retlst := make([]interface{}, 0)
        mjb := (jobpool.MemMap[id]).(*module.MetaJobMemBean)
        glog.Glog(LogF, fmt.Sprint(mjb.Id))  
        bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_CONF)
        defer bt.Close()

        strlist := bt.Scan()
        bt.Close()
        jobbn := new(module.MetaJobBean)
        bflg:=0
        for _, v := range strlist {
                for _, v1 := range v.(map[string]interface{}) {
                        err := json.Unmarshal([]byte(v1.(string)), &jobbn)
                        if err != nil {
                                glog.Glog(LogF, fmt.Sprint(err))
                                continue
                        }
                        if jobbn.Sys != mjb.Sys || jobbn.Job != mjb.Job {
                                continue
                        }
                        bflg=1
                        break
                }
                if bflg != 0 {
                     break
                }
        }
        conn, err := grpc.Dial(jobbn.Ip+":"+jobbn.Port, grpc.WithInsecure())
        if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
                glog.Glog(LogF, fmt.Sprintf("%v %v %v:%v did not connect: %v",cfile, cline,jobbn.Ip, jobbn.Port, err))
                util.ApiResponse(response, 700, fmt.Sprintf("%v:%v did not connect: %v",jobbn.Ip, jobbn.Port, err) , retlst)
                return
        }
        defer conn.Close()
        t := gproto.NewSlaverClient(conn)
        mjwb := new(module.MetaJobWorkerBean)
        mjwb.Id = mjb.Id
        mjwb.Sys = mjb.Sys
        mjwb.Job = mjb.Job
        timeStr := time.Now().Format("2006-01-02 15:04:05")
        mjwb.StartTime = timeStr
        jsonstr, _ := json.Marshal(mjwb)
        if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
                glog.Glog(LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
                util.ApiResponse(response, 700, fmt.Sprintf("%v",err) , retlst)
                return
        }
        tr, err := t.JobStop(context.Background(), &gproto.Req{JsonStr: string(jsonstr)})
        if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
                glog.Glog(LogF, fmt.Sprintf("%v %v could not greet: %v",cfile, cline ,err))
                util.ApiResponse(response, 700, fmt.Sprintf("%v",err) , retlst)
                return
        }
        //change job status
        if tr.Status_Code == 200 {
                glog.Glog(LogF, fmt.Sprint(tr.Status_Txt))
        }
        jobStatusUpdate(mjb.Sys,mjb.Job,util.SYS_STATUS_STOP,"et","no")
        util.ApiResponse(response, 200, "", retlst)
}


func jobStatusUpdate(sys string,job string,status string,code string,id string) bool {
        url := fmt.Sprintf("http://%v:%v/mst/job/status/update?accesstoken=%v&sys=%v&job=%v&status=%v&code=%v&id=%v", MstIp, MstPort, AccessToken, sys, job, status,code,id)
        jsonstr, err := util.Api_RequestPost(url, "{}")
        if err != nil {
                _, cfile, cline, _ := runtime.Caller(1)
                glog.Glog(LogF, fmt.Sprintf("%v %v %v",cfile, cline,err))
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

func JobTimeWindowAddHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode.%v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}
	jobtwb := new(module.MetaJobTimeWindowBean)
	jobtwb.Sys = sys
	jobtwb.Job = job
	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_TIMEWINDOW)
	defer bt.Close()
	jobtwb.Allow = "Y"
	jobtwb.Enable = "1"
	jobtwb.StartHour = 0
	jobtwb.EndHour = 23
	jsonstr, _ := json.Marshal(jobtwb)
	err = bt.Set(jobtwb.Sys+"."+jobtwb.Job, string(jsonstr))
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("data in db update error.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("data in db update error.%v", err), nil)
		return
	}
	util.ApiResponse(response, 200, "", nil)
}

func JobTimeWindowGetHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode: %v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_TIMEWINDOW)
	defer bt.Close()
	retlst := make([]interface{}, 0)
	jobn := bt.Get(sys + "." + job)
	if jobn != nil {
		jobbn := new(module.MetaJobTimeWindowBean)
		err := json.Unmarshal([]byte(jobn.(string)), &jobbn)
		if err != nil {
			glog.Glog(LogF, fmt.Sprint(err))
			util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode: %v", err), nil)
			return
		}
		retlst = append(retlst, jobbn)
	}
	util.ApiResponse(response, 200, "", retlst)
}

func JobTimeWindowRemoveHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode: %v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_TIMEWINDOW)
	defer bt.Close()

	err = bt.Remove(sys + "." + job)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("rm data err.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("rm data err.%v", err), nil)
		return
	}
	util.ApiResponse(response, 200, "", nil)
}

func JobCmdAddHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode.%v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}
	jobcmd := new(module.MetaJobCmdBean)
	jobcmd.Sys = sys
	jobcmd.Job = job
	jobcmd.Cmd = "bash /home/k8s/Go/ws/src/github.com/lhzd863/autoflow/slv/tmp/t.sh"
	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_CMD)
	defer bt.Close()
	jobcmd.Enable = "1"
	jsonstr, _ := json.Marshal(jobcmd)
	err = bt.Set(jobcmd.Sys+"."+jobcmd.Job, string(jsonstr))
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("data in db update error.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("data in db update error.%v", err), nil)
		return
	}
	util.ApiResponse(response, 200, "", nil)
}

func JobCmdGetHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode: %v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_CMD)
	defer bt.Close()
	retlst := make([]interface{}, 0)
	jobn := bt.Get(sys + "." + job)
	if jobn != nil {
		jobbn := new(module.MetaJobCmdBean)
		err := json.Unmarshal([]byte(jobn.(string)), &jobbn)
		if err != nil {
			glog.Glog(LogF, fmt.Sprint(err))
			util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode: %v", err), nil)
			return
		}
		retlst = append(retlst, jobbn)
	}
	util.ApiResponse(response, 200, "", retlst)
}

func JobCmdRemoveHandler(response http.ResponseWriter, request *http.Request) {
	//glog.Glog(LogF, "Method:"+fmt.Sprint(request.Method))
	if request.Method != "POST" {
		glog.Glog(LogF, "706 method is not post")
		util.ApiResponse(response, 706, "method is not post", nil)
		return
	}
	err := globalOauth(response, request)
	if err != nil {
		glog.Glog(LogF, "700 Not Authorized")
		util.ApiResponse(response, 700, "Not Authorized", nil)
		return
	}
	reqParams, err := util.NewReqParams(request)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Failed to decode: %v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("Failed to decode: %v", err), nil)
		return
	}
	sys, err := reqParams.Get("sys")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get sys parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get sys parameter failed.%v", err), nil)
		return
	}
	job, err := reqParams.Get("job")
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get job parameter failed.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("get job parameter failed.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(HomeDir+"/"+FlowId+"/"+FlowId+".db", util.SYS_TB_JOB_CMD)
	defer bt.Close()

	err = bt.Remove(sys + "." + job)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("rm data err.%v", err))
		util.ApiResponse(response, 700, fmt.Sprintf("rm data err.%v", err), nil)
		return
	}
	util.ApiResponse(response, 200, "", nil)
}
