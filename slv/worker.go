package slv

import (
	"bufio"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"io"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lhzd863/autoflow/internal/db"
	"github.com/lhzd863/autoflow/internal/glog"
	"github.com/lhzd863/autoflow/internal/gproto"
	"github.com/lhzd863/autoflow/internal/module"
	"github.com/lhzd863/autoflow/internal/util"
)

var (
	LogF          string
	HomeDir       string
	AccessToken   string
	ApiServerIp   string
	ApiServerPort string
	jobpool       = db.NewMemDB()
	ProcessNum    int
	Ip            string
	Port          string
	SlaveId       string
)

// 业务实现方法的容器
type SServer struct {
	waitGroup util.WaitGroupWrapper
}

func NewSServer(paraMap map[string]interface{}) *SServer {
	SlaveId = paraMap["slaveid"].(string)
	Ip = paraMap["ip"].(string)
	Port = paraMap["port"].(string)
	HomeDir = paraMap["homedir"].(string)
	AccessToken = paraMap["accesstoken"].(string)
	ApiServerIp = paraMap["apiserverip"].(string)
	ApiServerPort = paraMap["apiserverport"].(string)
	NumStr := paraMap["processnum"].(string)
	ProcessNum, _ = strconv.Atoi(NumStr)
	LogF = HomeDir + "/ms_${" + util.ENV_VAR_DATE + "}.log"
	m := &SServer{}
	return m

}

func (s *SServer) DoCmd(ctx context.Context, in *gproto.Req) (*gproto.Res, error) {
	fmt.Println("MD5方法请求JSON:" + in.JsonStr)
	return &gproto.Res{Status_Txt: "", Status_Code: 200, Data: "MD5 :" + fmt.Sprintf("%x", md5.Sum([]byte(in.JsonStr)))}, nil
}

func (s *SServer) Ping(ctx context.Context, in *gproto.Req) (*gproto.Res, error) {
	return &gproto.Res{Status_Txt: "", Status_Code: 200, Data: "{}"}, nil
}

func (s *SServer) JobStart(ctx context.Context, in *gproto.Req) (*gproto.Res, error) {
	mjs := new(module.MetaJobWorkerBean)
	err := json.Unmarshal([]byte(in.JsonStr), &mjs)
	if err != nil {
		_, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(LogF, fmt.Sprintf("%v %v %v", cfile, cline, err))
	}
	err = s.Executesh(mjs)
	var status_code int32
	status_code = 200
	status_txt := ""
	if err != nil {
		status_code = 799
		status_txt = fmt.Sprint(err)
	}
	fmt.Println("MD5方法请求JSON:" + in.JsonStr)
	return &gproto.Res{Status_Txt: status_txt, Status_Code: status_code, Data: "{}"}, nil
}

func (s *SServer) Executesh(job *module.MetaJobWorkerBean) error {
	logf := fmt.Sprintf("%v/LOG/%v/%v", HomeDir, job.FlowId, job.Sys)
	exist, err := util.PathExists(logf)
	if err != nil {
		return fmt.Errorf("failed to path exists: %v", err)
	}
	if !exist {
		os.MkdirAll(logf, os.ModePerm)
	}
	mjm := new(module.MetaJobMemBean)
	mjm.Id = job.Id
	mjm.Sys = job.Sys
	mjm.Job = job.Job
	timeStr := time.Now().Format("2006-01-02 15:04:05")
	mjm.CreateTime = timeStr
	mjm.Enable = "1"
	jobpool.Add(job.Id, mjm)
	for i := 0; i < len(job.RunningCmd); i++ {
		timeStr := time.Now().Format("20060102150405")
		jobLogF := fmt.Sprintf("%v/%v_%v_%v_%v.log", logf, strings.ToLower(job.Job), i, job.RunningTime, timeStr)
		cmd := exec.Command("/bin/bash", "-c", job.RunningCmd[i].(string))
		mjmt := jobpool.Get(job.Id).(*module.MetaJobMemBean)
		mjmt.Cmd = cmd
		timeStr = time.Now().Format("2006-01-02 15:04:05")
		mjmt.UpdateTime = timeStr
		jobpool.Add(job.Id, mjmt)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			_, cfile, cline, _ := runtime.Caller(1)
			glog.Glog(jobLogF, fmt.Sprintf("%v %v %v", cfile, cline, err))
			return fmt.Errorf("%v", err)
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			_, cfile, cline, _ := runtime.Caller(1)
			glog.Glog(jobLogF, fmt.Sprintf("%v %v %v", cfile, cline, err))
			return fmt.Errorf("%v", err)
		}
		cmd.Start()
		reader := bufio.NewReader(stdout)
		go func() {
			for {
				line, err2 := reader.ReadString('\n')
				if err2 != nil || io.EOF == err2 {
					break
				}
				glog.Glog(jobLogF, fmt.Sprintf("%v", line))
			}
		}()
		readererr := bufio.NewReader(stderr)
		go func() {
			for {
				line, err2 := readererr.ReadString('\n')
				if err2 != nil || io.EOF == err2 {
					break
				}
				glog.Glog(jobLogF, fmt.Sprintf("%v", line))
			}
		}()

		cmd.Wait()
		retcd := string(fmt.Sprintf("%v", cmd.ProcessState.Sys().(syscall.WaitStatus).ExitStatus()))
		retcd = strings.Replace(retcd, " ", "", -1)
		retcd = strings.Replace(retcd, "\n", "", -1)
		if retcd != "0" {
			glog.Glog(jobLogF, fmt.Sprintf("%v", retcd))
			return fmt.Errorf("%v", retcd)
		}
	}
	jobpool.Remove(job.Id)
	return nil
}

// 为server定义 DoMD5 方法 内部处理请求并返回结果
// 参数 (context.Context[固定], *test.Req[相应接口定义的请求参数])
// 返回 (*test.Res[相应接口定义的返回参数，必须用指针], error)
func (s *SServer) JobStop(ctx context.Context, in *gproto.Req) (*gproto.Res, error) {
	mjs := new(module.MetaJobWorkerBean)
	err := json.Unmarshal([]byte(in.JsonStr), &mjs)
	if err != nil {
		_, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(LogF, fmt.Sprintf("%v %v %v", cfile, cline, err))
	}
	for k := range jobpool.MemMap {
		jpo := (jobpool.MemMap[k]).(*module.MetaJobMemBean)
		if k == mjs.Id {
			jpo.Cmd.Process.Kill()
		}
	}
	glog.Glog(LogF, fmt.Sprintf("%v,%v,%v has stopped.", mjs.Id, mjs.Sys, mjs.Job))
	return &gproto.Res{Status_Txt: "", Status_Code: 200, Data: "MD5 :" + fmt.Sprintf("%x", md5.Sum([]byte(in.JsonStr)))}, nil
}

func (s *SServer) JobStatus(ctx context.Context, in *gproto.Req) (*gproto.Res, error) {
	mjs := new(module.MetaJobWorkerBean)
	err := json.Unmarshal([]byte(in.JsonStr), &mjs)
	if err != nil {
		_, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(LogF, fmt.Sprintf("%v %v %v", cfile, cline, err))
	}
	var jsonstr []byte
	for k := range jobpool.MemMap {
		jpo := (jobpool.MemMap[k]).(*module.MetaJobMemBean)
		if k == mjs.Id {
			jsonstr, _ = json.Marshal(jpo)
		}
	}
	return &gproto.Res{Status_Txt: "", Status_Code: 200, Data: string(jsonstr)}, nil
}

func (s *SServer) Main() bool {
	go func() {
		for {
			ret := s.Register()
			if !ret {
				glog.Glog(LogF, "register slave fail.")
			}
			time.Sleep(time.Duration(300) * time.Second)
		}
	}()

	lis, err := net.Listen("tcp", ":"+Port) //监听所有网卡8028端口的TCP连接
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("监听失败: %v", err))
		return false
	}
	ss := grpc.NewServer() //创建gRPC服务

	/**注册接口服务
	 * 以定义proto时的service为单位注册，服务中可以有多个方法
	 * (proto编译时会为每个service生成Register***Server方法)
	 * 包.注册服务方法(gRpc服务实例，包含接口方法的结构体[指针])
	 */
	gproto.RegisterSlaverServer(ss, &SServer{})
	/**如果有可以注册多个接口服务,结构体要实现对应的接口方法
	 * user.RegisterLoginServer(s, &server{})
	 * minMovie.RegisterFbiServer(s, &server{})
	 */
	// 在gRPC服务器上注册反射服务
	reflection.Register(ss)
	// 将监听交给gRPC服务处理
	err = ss.Serve(lis)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("failed to serve: %v", err))
		return false
	}
	return true
}

func (s *SServer) Register() bool {
	glog.Glog(LogF, fmt.Sprintf("Register node %v, %v", Ip, Port))
	url := fmt.Sprintf("http://%v:%v/api/v1/slave/add?accesstoken=%v", ApiServerIp, ApiServerPort, AccessToken)
	jpb := fmt.Sprintf("{\"ip\":\"%v\",\"port\":\"%v\",\"slaveid\":\"%v\",\"maxcnt\":\"%v\",\"runningcnt\":\"%v\",\"currentcnt\":\"%v\"}", Ip, Port, SlaveId, ProcessNum, len(jobpool.MemMap), "0")
	jsonstr, err := util.Api_RequestPost(url, jpb)
	if err != nil {
		glog.Glog(LogF, fmt.Sprint(err))
		return false
	}
	retbn1 := new(module.RetBean)
	err = json.Unmarshal([]byte(jsonstr), &retbn1)
	if err != nil {
		glog.Glog(LogF, fmt.Sprint(err))
		return false
	}
	if retbn1.Status_Code != 200 {
		glog.Glog(LogF, fmt.Sprintf("post url return status code:%v", retbn1.Status_Code))
		return false
	}
	return true
}
