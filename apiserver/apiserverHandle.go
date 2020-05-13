package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/emicklei/go-restful"
	restfulspec "github.com/emicklei/go-restful-openapi"
	"github.com/go-openapi/spec"

	"github.com/lhzd863/autoflow/internal/db"
	"github.com/lhzd863/autoflow/internal/glog"
	"github.com/lhzd863/autoflow/internal/jwt"
	"github.com/lhzd863/autoflow/internal/module"
	"github.com/lhzd863/autoflow/internal/util"
	"github.com/lhzd863/autoflow/mst"

	"github.com/satori/go.uuid"

)

type MetaInstancePortBean struct {
        Ip           string       `json:"ip"`
        Port         string       `json:"port"`
        FlowId       string       `json:"flowid"`
        HttpListener net.Listener `json:"httplistener"`
        Mt           *mst.Mst     `json:"mst"`
        Enable       string       `json:"enable"`
}

// JobResource is the REST layer to the User domain
type ResponseResource struct {
	// normally one would use DAO (data access object)
	sync.RWMutex
	Data map[string]interface{}
}

// WebService creates a new service that can handle REST requests for User resources.
func (rrs ResponseResource) WebService() *restful.WebService {
	ws := new(restful.WebService)
	ws.
		Path("/api/v1").
		Consumes("*/*").
		Produces(restful.MIME_JSON, restful.MIME_JSON) // you can specify this per route as well

	tags := []string{"system"}

	ws.Route(ws.GET("/health").To(rrs.HealthHandler).
		// docs
		Doc("Health").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

	ws.Route(ws.POST("/login").To(rrs.LoginHandler).
		// docs
		Doc("login info").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

        ws.Route(ws.POST("/sys/lsport").To(rrs.SysListPortHandler).
                // docs
                Doc("list port").
                Metadata(restfulspec.KeyOpenAPITags, tags).
                Writes(ResponseResource{}). // on the response
                Returns(200, "OK", ResponseResource{}).
                Returns(404, "Not Found", nil))
        
        tags = []string{"image"}
	ws.Route(ws.POST("/image/add").To(rrs.ImageAddHandler).
		// docs
		Doc("添加镜像文件").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

	ws.Route(ws.POST("/image/rm").To(rrs.ImageRemoveHandler).
		// docs
		Doc("删除镜像文件").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

	ws.Route(ws.POST("/image/ls").To(rrs.ImageListHandler).
		// docs
		Doc("镜像文件列表").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

	ws.Route(ws.POST("/image/get").To(rrs.ImageGetHandler).
		// docs
		Doc("镜像文件获取").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))
        
        ws.Route(ws.POST("/image/update").To(rrs.ImageUpdateHandler).
                // docs
                Doc("镜像文件修改").
                Metadata(restfulspec.KeyOpenAPITags, tags).
                Writes(ResponseResource{}). // on the response
                Returns(200, "OK", ResponseResource{}).
                Returns(404, "Not Found", nil))
        
        tags = []string{"instance"}
	ws.Route(ws.POST("/instance/create").To(rrs.InstanceCreateHandler).
		// docs
		Doc("实例创建").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

	ws.Route(ws.POST("/instance/start").To(rrs.InstanceStartHandler).
		// docs
		Doc("实例开启").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

	ws.Route(ws.POST("/instance/stop").To(rrs.InstanceStopHandler).
		// docs
		Doc("实例停止").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

        ws.Route(ws.POST("/instance/ls").To(rrs.InstanceListHandler).
                // docs
                Doc("实例列表").
                Metadata(restfulspec.KeyOpenAPITags, tags).
                Writes(ResponseResource{}). // on the response
                Returns(200, "OK", ResponseResource{}).
                Returns(404, "Not Found", nil))

        tags = []string{"flow"}
	ws.Route(ws.POST("/flow/ls").To(rrs.FlowListHandler).
		// docs
		Doc("流列表").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

	ws.Route(ws.POST("/flow/get").To(rrs.FlowGetHandler).
		// docs
		Doc("流获取").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

	ws.Route(ws.POST("/flow/status/update").To(rrs.FlowStatusUpdateHandler).
		// docs
		Doc("流状态更新").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

        tags = []string{"job"}
	ws.Route(ws.POST("/job/pool/add").To(rrs.JobPoolAddHandler).
		// docs
		Doc("作业池添加").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

	ws.Route(ws.POST("/job/pool/rm").To(rrs.JobPoolRemoveHandler).
		// docs
		Doc("作业池删除").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

	ws.Route(ws.POST("/job/pool/get").To(rrs.JobPoolGetHandler).
		// docs
		Doc("作业池获取").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

	ws.Route(ws.POST("/job/pool/ls").To(rrs.JobPoolListHandler).
		// docs
		Doc("作业池列表").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

        tags = []string{"slave"}
	ws.Route(ws.POST("/slave/add").To(rrs.SlaveAddHandler).
		// docs
		Doc("节点注册").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

	ws.Route(ws.POST("/slave/rm").To(rrs.SlaveRemoveHandler).
		// docs
		Doc("节点删除").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

	ws.Route(ws.POST("/slave/ls").To(rrs.SlaveListHandler).
		// docs
		Doc("节点列表").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

	ws.Route(ws.POST("/slave/get").To(rrs.SlaveGetHandler).
		// docs
		Doc("节点获取").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

	ws.Route(ws.POST("/slave/cnt/add").To(rrs.SlaveCntAddHandler).
		// docs
		Doc("节点作业数增加").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

	ws.Route(ws.POST("/slave/cnt/exec").To(rrs.SlaveExecCntHandler).
		// docs
		Doc("节点执行作业").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Writes(ResponseResource{}). // on the response
		Returns(200, "OK", ResponseResource{}).
		Returns(404, "Not Found", nil))

        ws.Route(ws.POST("/job/ls").To(rrs.jobListHandleRouter).
                // docs
                Doc("作业列表").
                Metadata(restfulspec.KeyOpenAPITags, tags).
                Writes(ResponseResource{}). // on the response
                Returns(200, "OK", ResponseResource{}).
                Returns(404, "Not Found", nil))

	return ws
}

func (rrs *ResponseResource) HealthHandler(request *restful.Request, response *restful.Response) {
	list := []module.RetBean{}
	list = append(list, module.RetBean{Code: "200", Status: "ok", Err: "", Data: "{}"})
	response.WriteEntity(list)
}

func (rrs *ResponseResource) LoginHandler(request *restful.Request, response *restful.Response) {
	list := []module.RetBean{}
	list = append(list, module.RetBean{Code: "200", Status: "ok", Err: "", Data: "{}"})
	response.WriteEntity(list)
}

func (rrs *ResponseResource) ImageAddHandler(request *restful.Request, response *restful.Response) {
	reqParams, err := url.ParseQuery(request.Request.URL.RawQuery)
	if err != nil {
		glog.Glog(LogF, fmt.Sprint(err))
		//response.WriteErrorString(401, "parse url parameter err.")
		util.ApiResponse(response.ResponseWriter, 700, "parse url parameter err.", nil)
		return
	}

	username, err := util.JwtAccessTokenUserName(fmt.Sprint(reqParams["accesstoken"][0]), conf.JwtKey)
	if err != nil {
		glog.Glog(LogF, fmt.Sprint(err))
		//response.WriteErrorString(401, "accesstoken parse username err.")
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("accesstoken parse username err.%v", err), nil)
		return
	}

	imagebean := new(module.MetaJobImageBean)
	err = request.ReadEntity(&imagebean)
	if err != nil {
		glog.Glog(LogF, fmt.Sprint(err))
		//response.WriteErrorString(http.StatusNotFound, "Parse json error.")
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("Parse json error.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_JOB_IMAGE)
	defer bt.Close()

	timeStr := time.Now().Format("2006-01-02 15:04:05")
	imagebean.CreateTime = timeStr
	imagebean.User = username
	//imagebean.Enable = "1"

	u1 := uuid.Must(uuid.NewV4())
	imageid := fmt.Sprint(u1)
	imagebean.ImageId = imageid

	jsonstr, _ := json.Marshal(imagebean)
	err = bt.Set(imageid, string(jsonstr))
	if err != nil {
		glog.Glog(LogF, fmt.Sprint(err))
		//response.WriteErrorString(401, "data in db update error.")
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("data in db update error.%v", err), nil)
		return
	}

	//response.WriteEntity(list)
	util.ApiResponse(response.ResponseWriter, 200, "", nil)
}

func (rrs *ResponseResource) ImageUpdateHandler(request *restful.Request, response *restful.Response) {
        reqParams, err := url.ParseQuery(request.Request.URL.RawQuery)
        if err != nil {
                glog.Glog(LogF, fmt.Sprint(err))
                util.ApiResponse(response.ResponseWriter, 700, "parse url parameter err.", nil)
                return
        }

        username, err := util.JwtAccessTokenUserName(fmt.Sprint(reqParams["accesstoken"][0]), conf.JwtKey)
        if err != nil {
                glog.Glog(LogF, fmt.Sprint(err))
                util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("accesstoken parse username err.%v", err), nil)
                return
        }

        imagebean := new(module.MetaJobImageBean)
        err = request.ReadEntity(&imagebean)
        if err != nil {
                glog.Glog(LogF, fmt.Sprint(err))
                util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("Parse json error.%v", err), nil)
                return
        }
        bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_JOB_IMAGE)
        defer bt.Close()

        timeStr := time.Now().Format("2006-01-02 15:04:05")
        imagebean.CreateTime = timeStr
        imagebean.User = username
		
		if len(imagebean.ImageId) < 1 {
                util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("image id is not exists."), nil)
                return
		}

        jsonstr, _ := json.Marshal(imagebean)
        err = bt.Set(imagebean.ImageId, string(jsonstr))
        if err != nil {
                glog.Glog(LogF, fmt.Sprint(err))
                util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("data in db update error.%v", err), nil)
                return
        }

        util.ApiResponse(response.ResponseWriter, 200, "", nil)
}

func (rrs *ResponseResource) ImageRemoveHandler(request *restful.Request, response *restful.Response) {

	imagebean := new(module.MetaJobImageBean)
	err := request.ReadEntity(&imagebean)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Parse json error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("Parse json error.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_JOB_IMAGE)
	defer bt.Close()

	err = bt.Remove(imagebean.ImageId)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("data in db remove error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("data in db remove error.%v", err), nil)
		return
	}
	util.ApiResponse(response.ResponseWriter, 200, "", nil)
}

func (rrs *ResponseResource) ImageListHandler(request *restful.Request, response *restful.Response) {
	bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_JOB_IMAGE)
	defer bt.Close()

	strlist := bt.Scan()
	retlst := make([]interface{}, 0)
	for _, v := range strlist {
		for _, v1 := range v.(map[string]interface{}) {
			masb := new(module.MetaJobImageBean)
			err := json.Unmarshal([]byte(v1.(string)), &masb)
			if err != nil {
				glog.Glog(LogF, fmt.Sprint(err))
				continue
			}
			retlst = append(retlst, masb)
		}
	}
	util.ApiResponse(response.ResponseWriter, 200, "", retlst)
}

func (rrs *ResponseResource) FlowListHandler(request *restful.Request, response *restful.Response) {
	bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_JOB_FLOW)
	defer bt.Close()

	strlist := bt.Scan()
	retlst := make([]interface{}, 0)
	for _, v := range strlist {
		for _, v1 := range v.(map[string]interface{}) {
			mjfb := new(module.MetaJobFlowBean)
			err := json.Unmarshal([]byte(v1.(string)), &mjfb)
			if err != nil {
				glog.Glog(LogF, fmt.Sprint(err))
				continue
			}
			retlst = append(retlst, mjfb)
		}
	}
	util.ApiResponse(response.ResponseWriter, 200, "", retlst)
}

func (rrs *ResponseResource) FlowGetHandler(request *restful.Request, response *restful.Response) {
	mjfbean := new(module.MetaJobFlowBean)
	err := request.ReadEntity(&mjfbean)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("parse json error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("parse json error.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_JOB_FLOW)
	defer bt.Close()

	retlst := make([]interface{}, 0)
	ib := bt.Get(mjfbean.FlowId)
	if ib != nil {
		mjfb := new(module.MetaJobFlowBean)
		err := json.Unmarshal([]byte(ib.(string)), &mjfb)
		if err != nil {
			glog.Glog(LogF, fmt.Sprint(err))
		}
		retlst = append(retlst, mjfb)
	}
	util.ApiResponse(response.ResponseWriter, 200, "", retlst)
}

func (rrs *ResponseResource) ImageGetHandler(request *restful.Request, response *restful.Response) {
	imagebean := new(module.MetaJobImageBean)
	err := request.ReadEntity(&imagebean)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("parse json error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("parse json error.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_JOB_IMAGE)
	defer bt.Close()

	retlst := make([]interface{}, 0)
	ib := bt.Get(imagebean.ImageId)
	if ib != nil {
		masb := new(module.MetaJobImageBean)
		err := json.Unmarshal([]byte(ib.(string)), &masb)
		if err != nil {
			glog.Glog(LogF, fmt.Sprint(err))
		}
		retlst = append(retlst, masb)
	}
	util.ApiResponse(response.ResponseWriter, 200, "", retlst)
}

func (rrs *ResponseResource) InstanceCreateHandler(request *restful.Request, response *restful.Response) {
	reqParams, err := url.ParseQuery(request.Request.URL.RawQuery)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("parse para error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("parse para error.%v", err), nil)
		return
	}

	username, err := util.JwtAccessTokenUserName(fmt.Sprint(reqParams["accesstoken"][0]), conf.JwtKey)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get username error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("get username error.%v", err), nil)
		return
	}
	tipb, err := getMstPort()
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get mst port error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("get mst port error.%v", err), nil)
		return
	}

	flowbean := new(module.MetaJobFlowBean)
	err = request.ReadEntity(&flowbean)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("parse json error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("parse json error.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_JOB_IMAGE)
	defer bt.Close()

	ib := bt.Get(flowbean.ImageId)
	bt.Close()
	if ib == nil {
		glog.Glog(LogF, "no data result")
		util.ApiResponse(response.ResponseWriter, 700, "no data result", nil)
		return
	}
	masb := new(module.MetaJobImageBean)
	err = json.Unmarshal([]byte(ib.(string)), &masb)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("parse json error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("parse json error.%v", err), nil)
		return
	}

	flowinstbean := new(module.MetaJobFlowBean)
	timeStr := time.Now().Format("2006-01-02 15:04:05")
	flowinstbean.StartTime = timeStr
	flowinstbean.ImageId = masb.ImageId
	flowinstbean.Enable = "1"
	flowinstbean.User = username
	flowinstbean.Status = util.SYS_STATUS_READY
	flowinstbean.DbStore = masb.DbStore
	flowinstbean.Ip = tipb.Ip
	flowinstbean.Port = tipb.Port
	flowinstbean.HomeDir = conf.HomeDir

	frid := util.RandStringBytes(6)
	timeStrId := time.Now().Format("20060102150405")
	fid := fmt.Sprintf("%v%v", timeStrId, frid)
	flowinstbean.FlowId = fid
	tipb.FlowId = fid

	if ok, err := util.PathExists(conf.HomeDir + "/" + flowinstbean.FlowId); !ok {
		os.Mkdir(conf.HomeDir+"/"+flowinstbean.FlowId, os.ModePerm)
	} else {
		glog.Glog(LogF, fmt.Sprint(err))
	}

	if !util.FileExist(conf.HomeDir + "/" + flowinstbean.FlowId + "/" + flowinstbean.FlowId + ".db") {
		util.Copy(masb.DbStore, conf.HomeDir+"/"+flowinstbean.FlowId+"/"+flowinstbean.FlowId+".db")
	}

	bt1 := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_JOB_FLOW)
	defer bt1.Close()

	jsonstr, _ := json.Marshal(flowinstbean)
	err = bt1.Set(flowinstbean.FlowId, string(jsonstr))
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("data in db update error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("data in db update error.%v", err), nil)
		return
	}
	go func() {
		m := make(map[string]interface{})
		m["flowid"] = flowinstbean.FlowId
		m["apiserverip"] = conf.ApiServerIp
		m["apiserverport"] = conf.ApiServerPort
		m["port"] = tipb.Port
		m["ip"] = tipb.Ip
		m["jwtkey"] = conf.JwtKey
		m["homedir"] = conf.HomeDir
		m["accesstoken"] = conf.AccessToken
		pnum := conf.MstMaxPort - conf.MstMinPort
		m["processnum"] = fmt.Sprint(pnum)
		httpListener, err := net.Listen("tcp", "0.0.0.0:"+tipb.Port)
		if err != nil {
			glog.Glog(LogF, fmt.Sprintf("FATAL: listen (%s) failed - %s", conf.MstIp, err.Error()))
		}
		//tipb.HttpListener = httpListener
		nm := mst.NewMst(m, httpListener)
		nm.Main()
                tipb.Mt = nm
                tipb.Mt.HttpListener = httpListener
	}()
	tipb.Enable = "1"
	//mstMap[tipb.Ip+":"+tipb.Port] = tipb
	mstMap.Add(tipb.Ip+":"+tipb.Port, tipb)
	util.ApiResponse(response.ResponseWriter, 200, "", nil)
}

func (rrs *ResponseResource) InstanceStartHandler(request *restful.Request, response *restful.Response) {
	reqParams, err := url.ParseQuery(request.Request.URL.RawQuery)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("parse para error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("parse para error.%v", err), nil)
		return
	}

	username, err := util.JwtAccessTokenUserName(fmt.Sprint(reqParams["accesstoken"][0]), conf.JwtKey)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get username error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("get username error.%v", err), nil)
		return
	}
	mjfbean := new(module.MetaJobFlowBean)
	err = request.ReadEntity(&mjfbean)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("parse json error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("parse json error.%v", err), nil)
		return
	}

	bt2 := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_JOB_FLOW)
	defer bt2.Close()
	fb0 := bt2.Get(mjfbean.FlowId)
	if fb0 == nil {
		glog.Glog(LogF, fmt.Sprintf("flow %v not exists,start flow fail.", mjfbean.FlowId))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("flow %v not exists,start flow fail.", mjfbean.FlowId), nil)
		return
	}
	bt2.Close()
	fb := new(module.MetaJobFlowBean)
	err = json.Unmarshal([]byte(fb0.(string)), &fb)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("parse json error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("parse json error.%v", err), nil)
		return
	}
	if fb == nil {
		glog.Glog(LogF, "no data result")
		util.ApiResponse(response.ResponseWriter, 700, "no data result", nil)
		return
	}
	tipb1, _ := getFlowListener(mjfbean.FlowId)
	if tipb1 != nil {
		glog.Glog(LogF, "flow "+mjfbean.FlowId+" is running,no restart flow exit")
		util.ApiResponse(response.ResponseWriter, 700, "flow "+mjfbean.FlowId+" is running,no restart flow exit", nil)
		return
	}
	tipb, err := getMstPort()
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get mst port error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("get mst port error.%v", err), nil)
		return
	}
	tipb.FlowId = mjfbean.FlowId

	timeStr := time.Now().Format("2006-01-02 15:04:05")
	fb.StartTime = timeStr
	fb.User = username
	fb.Status = util.SYS_STATUS_RUNNING
	fb.Ip = tipb.Ip
	fb.Port = tipb.Port

	if ok, _ := util.PathExists(fb.HomeDir + "/" + fb.FlowId); !ok {
		glog.Glog(LogF, "Home dir "+fb.HomeDir+"/"+fb.FlowId+" not exists.")
		util.ApiResponse(response.ResponseWriter, 700, "Home dir "+fb.HomeDir+"/"+fb.FlowId+" not exists.", nil)
		return
	}

	if !util.FileExist(fb.HomeDir + "/" + fb.FlowId + "/" + fb.FlowId + ".db") {
		glog.Glog(LogF, fb.HomeDir+"/"+fb.FlowId+"/"+fb.FlowId+".db not exists.")
		util.ApiResponse(response.ResponseWriter, 700, fb.HomeDir+"/"+fb.FlowId+"/"+fb.FlowId+".db not exists.", nil)
		return
	}

	bt1 := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_JOB_FLOW)
	defer bt1.Close()

	jsonstr, _ := json.Marshal(fb)
	err = bt1.Set(fb.FlowId, string(jsonstr))
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("data in db update error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("data in db update error.%v", err), nil)
		return
	}

	go func() {
		m := make(map[string]interface{})
		m["flowid"] = fb.FlowId
		m["apiserverip"] = conf.ApiServerIp
		m["apiserverport"] = conf.ApiServerPort
		m["port"] = tipb.Port
		m["ip"] = tipb.Ip
		m["jwtkey"] = conf.JwtKey
		m["homedir"] = fb.HomeDir
		m["accesstoken"] = conf.AccessToken
		pnum := conf.MstMaxPort - conf.MstMinPort
		m["processnum"] = fmt.Sprint(pnum)
		httpListener, err := net.Listen("tcp", "0.0.0.0:"+tipb.Port)
		if err != nil {
			glog.Glog(LogF, fmt.Sprintf("FATAL: listen (%s) failed - %s", conf.MstIp, err.Error()))
		}
		//tipb.HttpListener = httpListener
		nm := mst.NewMst(m, httpListener)
		nm.Main()
                tipb.Mt = nm
                tipb.Mt.HttpListener = httpListener
	}()
	tipb.Enable = "1"
	//mstMap[tipb.Ip+":"+tipb.Port] = tipb
	mstMap.Add(tipb.Ip+":"+tipb.Port, tipb)
	util.ApiResponse(response.ResponseWriter, 200, "", nil)
}

func (rrs *ResponseResource) InstanceStopHandler(request *restful.Request, response *restful.Response) {
	mjfbean := new(module.MetaJobFlowBean)
	err := request.ReadEntity(&mjfbean)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("parse json error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("parse json error.%v", err), nil)
		return
	}
	tipb, err := getFlowListener(mjfbean.FlowId)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("flow listener parse error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("flow listener parse error.%v", err), nil)
		return
	}
	if tipb.Mt !=nil && tipb.Mt.HttpListener != nil {
		glog.Glog(LogF, "stop flow "+mjfbean.FlowId)
		tipb.Mt.CloseServer()
                tipb.Mt.HttpListener.Close()
	}
	tipb.Enable = "0"
	tipb.FlowId = ""
	//tipb.HttpListener = nil
        tipb.Mt = nil
	//mstMap[tipb.Ip+":"+tipb.Port] = tipb
	mstMap.Add(tipb.Ip+":"+tipb.Port, tipb)

	bt2 := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_JOB_FLOW)
	defer bt2.Close()
	fb0 := bt2.Get(mjfbean.FlowId)
	fb := new(module.MetaJobFlowBean)
	err = json.Unmarshal([]byte(fb0.(string)), &fb)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("parse json error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("parse json error.%v", err), nil)
		return
	}
	fb.Status = util.SYS_STATUS_STOP
	jsonstr, _ := json.Marshal(fb)
	err = bt2.Set(fb.FlowId, string(jsonstr))
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("data in db update error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("data in db update error.%v", err), nil)
		return
	}
	util.ApiResponse(response.ResponseWriter, 200, "", nil)
}

func (rrs *ResponseResource) InstanceListHandler(request *restful.Request, response *restful.Response) {
        bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_JOB_FLOW)
        defer bt.Close()

        strlist := bt.Scan()
        retlst := make([]interface{}, 0)
        for _, v := range strlist {
                for _, v1 := range v.(map[string]interface{}) {
                        mjfb := new(module.MetaJobFlowBean)
                        err := json.Unmarshal([]byte(v1.(string)), &mjfb)
                        if err != nil {
                                glog.Glog(LogF, fmt.Sprint(err))
                                continue
                        }
                        retlst = append(retlst, mjfb)
                }
        }
        util.ApiResponse(response.ResponseWriter, 200, "", retlst)
}

func (rrs *ResponseResource) FlowStatusUpdateHandler(request *restful.Request, response *restful.Response) {
	reqParams, err := url.ParseQuery(request.Request.URL.RawQuery)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("parse para error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("parse para error.%v", err), nil)
		return
	}

	username, err := util.JwtAccessTokenUserName(fmt.Sprint(reqParams["accesstoken"][0]), conf.JwtKey)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("get username error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("get username error.%v", err), nil)
		return
	}
	mjfbean := new(module.MetaJobFlowBean)
	err = request.ReadEntity(&mjfbean)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("parse json error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("parse json error.%v", err), nil)
		return
	}

	bt2 := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_JOB_FLOW)
	defer bt2.Close()
	fb0 := bt2.Get(mjfbean.FlowId)
	fb := new(module.MetaJobFlowBean)
	err = json.Unmarshal([]byte(fb0.(string)), &fb)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("parse json error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("parse json error.%v", err), nil)
		return
	}
	fb.Status = mjfbean.Status
	fb.User = username
	jsonstr, _ := json.Marshal(fb)
	err = bt2.Set(fb.FlowId, string(jsonstr))
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("data in db update error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("data in db update error.%v", err), nil)
		return
	}
}

func (rrs *ResponseResource) SysListPortHandler(request *restful.Request, response *restful.Response) {
	//list := []*module.MetaInstancePortBean{}
        list := []*MetaInstancePortBean{}
	for k := range mstMap.MemMap {
		//mipb := (mstMap.MemMap[k]).(*module.MetaInstancePortBean)
                mipb := (mstMap.MemMap[k]).(*MetaInstancePortBean)
		list = append(list, mipb)
	}
	util.ApiResponse(response.ResponseWriter, 200, "", list)
}

func (rrs *ResponseResource) JobPoolAddHandler(request *restful.Request, response *restful.Response) {
	jpbean := new(module.MetaJobPoolBean)
	err := request.ReadEntity(&jpbean)
	if err != nil {
		glog.Glog(LogF, fmt.Sprint(err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("Parse json error.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_JOB_POOL)
	defer bt.Close()

	timeStr := time.Now().Format("2006-01-02 15:04:05")
	jpbean.StartTime = timeStr
	jpbean.Enable = "1"

	jsonstr, _ := json.Marshal(jpbean)
	err = bt.Set(jpbean.Sys+"."+jpbean.Job+"."+jpbean.FlowId, string(jsonstr))
	if err != nil {
		glog.Glog(LogF, fmt.Sprint(err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("data in db update error.%v", err), nil)
		return
	}

	util.ApiResponse(response.ResponseWriter, 200, "", nil)
}

func (rrs *ResponseResource) JobPoolGetHandler(request *restful.Request, response *restful.Response) {
	jpbean := new(module.MetaJobPoolBean)
	err := request.ReadEntity(&jpbean)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("parse json error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("parse json error.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_JOB_POOL)
	defer bt.Close()

	retlst := make([]interface{}, 0)
	jp := bt.Get(jpbean.Sys + "." + jpbean.Job + "." + jpbean.FlowId)
	if jp != nil {
		mjpb := new(module.MetaJobPoolBean)
		err := json.Unmarshal([]byte(jp.(string)), &mjpb)
		if err != nil {
			glog.Glog(LogF, fmt.Sprint(err))
		}
		retlst = append(retlst, mjpb)
	}
	util.ApiResponse(response.ResponseWriter, 200, "", retlst)
}

func (rrs *ResponseResource) JobPoolListHandler(request *restful.Request, response *restful.Response) {
	bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_JOB_POOL)
	defer bt.Close()

	strlist := bt.Scan()
	retlst := make([]interface{}, 0)
	for _, v := range strlist {
		for _, v1 := range v.(map[string]interface{}) {
			mjpb := new(module.MetaJobPoolBean)
			err := json.Unmarshal([]byte(v1.(string)), &mjpb)
			if err != nil {
				glog.Glog(LogF, fmt.Sprint(err))
				continue
			}
			retlst = append(retlst, mjpb)
		}
	}
	util.ApiResponse(response.ResponseWriter, 200, "", retlst)
}

func (rrs *ResponseResource) JobPoolRemoveHandler(request *restful.Request, response *restful.Response) {

	jpbean := new(module.MetaJobPoolBean)
	err := request.ReadEntity(&jpbean)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Parse json error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("Parse json error.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_JOB_POOL)
	defer bt.Close()

	err = bt.Remove(jpbean.Sys + "." + jpbean.Job + "." + jpbean.FlowId)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("data in db remove error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("data in db remove error.%v", err), nil)
		return
	}
	util.ApiResponse(response.ResponseWriter, 200, "", nil)
}

//func getMstPort() (*module.MetaInstancePortBean, error) {
func getMstPort() (*MetaInstancePortBean, error) {
	//var tipb *module.MetaInstancePortBean
        var tipb *MetaInstancePortBean
	flag := 0
	for k := range mstMap.MemMap {
		//masb := (mstMap.MemMap[k]).(*module.MetaInstancePortBean)
                masb := (mstMap.MemMap[k]).(*MetaInstancePortBean)
		if masb.Enable == "0" {
			flag = 1
			tipb = masb
			break
		}
	}
	if flag == 0 {
		return nil, errors.New("mst ip mapping no port,wait for next time.")
	}
	return tipb, nil
}

//func getFlowListener(flowid string) (*module.MetaInstancePortBean, error) {
func getFlowListener(flowid string) (*MetaInstancePortBean, error) {
        var tipb *MetaInstancePortBean
	//var tipb *module.MetaInstancePortBean
	flag := 0
	for k := range mstMap.MemMap {
		//masb := (mstMap.MemMap[k]).(*module.MetaInstancePortBean)
                masb := (mstMap.MemMap[k]).(*MetaInstancePortBean)
		if masb.Enable == "1" && masb.FlowId == flowid {
			flag = 1
			tipb = masb
			break
		}
	}
	if flag == 0 {
		return nil, errors.New("ip mapping no port,wait for next time.")
	}
	return tipb, nil
}

func (rrs *ResponseResource) SlaveAddHandler(request *restful.Request, response *restful.Response) {
	mjshbean := new(module.MetaJobSlaveHeartBean)
	err := request.ReadEntity(&mjshbean)
	if err != nil {
		glog.Glog(LogF, fmt.Sprint(err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("Parse json error.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_SLAVE_HEART)
	defer bt.Close()

	timeStr := time.Now().Format("2006-01-02 15:04:05")
	mjshbean.UpdateTime = timeStr
	mjshbean.Enable = "1"

	jsonstr, _ := json.Marshal(mjshbean)
	err = bt.Set(mjshbean.SlaveId, string(jsonstr))
	if err != nil {
		glog.Glog(LogF, fmt.Sprint(err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("data in db update error.%v", err), nil)
		return
	}
	util.ApiResponse(response.ResponseWriter, 200, "", nil)
}

func (rrs *ResponseResource) SlaveRemoveHandler(request *restful.Request, response *restful.Response) {
	mjshbean := new(module.MetaJobSlaveHeartBean)
	err := request.ReadEntity(&mjshbean)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("Parse json error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("Parse json error.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_SLAVE_HEART)
	defer bt.Close()

	err = bt.Remove(mjshbean.SlaveId)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("data in db remove error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("data in db remove error.%v", err), nil)
		return
	}
	util.ApiResponse(response.ResponseWriter, 200, "", nil)
}

func (rrs *ResponseResource) SlaveListHandler(request *restful.Request, response *restful.Response) {
	bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_SLAVE_HEART)
	defer bt.Close()

	strlist := bt.Scan()
	retlst := make([]interface{}, 0)
	for _, v := range strlist {
		for _, v1 := range v.(map[string]interface{}) {
			mjshb := new(module.MetaJobSlaveHeartBean)
			err := json.Unmarshal([]byte(v1.(string)), &mjshb)
			if err != nil {
				glog.Glog(LogF, fmt.Sprint(err))
				continue
			}
                        timeStr := time.Now().Format("2006-01-02 15:04:05")
                        loc, _ := time.LoadLocation("Local")
                        timeLayout := "2006-01-02 15:04:05"
                        stheTime, _ := time.ParseInLocation(timeLayout, mjshb.UpdateTime, loc)
                        sst := stheTime.Unix()
                        etheTime, _ := time.ParseInLocation(timeLayout, timeStr, loc)
                        est := etheTime.Unix()
                        if est-sst > 3600 {
                           glog.Glog(LogF, fmt.Sprint("%v timeout %v,%v set enable =0 .",mjshb.SlaveId,mjshb.Ip,mjshb.Port))
                           mjshb.Enable = "0"
                        }
			retlst = append(retlst, mjshb)
		}
	}
	util.ApiResponse(response.ResponseWriter, 200, "", retlst)
}

func (rrs *ResponseResource) SlaveGetHandler(request *restful.Request, response *restful.Response) {
	mjshbean := new(module.MetaJobSlaveHeartBean)
	err := request.ReadEntity(&mjshbean)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("parse json error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("parse json error.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_SLAVE_HEART)
	defer bt.Close()

	retlst := make([]interface{}, 0)
	mjsh := bt.Get(mjshbean.SlaveId)
	if mjsh != nil {
		mjshb := new(module.MetaJobSlaveHeartBean)
		err := json.Unmarshal([]byte(mjsh.(string)), &mjshb)
		if err != nil {
			glog.Glog(LogF, fmt.Sprint(err))
		}
		retlst = append(retlst, mjshb)
	}
	util.ApiResponse(response.ResponseWriter, 200, "", retlst)
}

func (rrs *ResponseResource) SlaveCntAddHandler(request *restful.Request, response *restful.Response) {
	mjshbean := new(module.MetaJobSlaveHeartBean)
	err := request.ReadEntity(&mjshbean)
	if err != nil {
		glog.Glog(LogF, fmt.Sprint(err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("Parse json error.%v", err), nil)
		return
	}
	bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_SLAVE_HEART)
	defer bt.Close()

	timeStr := time.Now().Format("2006-01-02 15:04:05")
	mjshbean.UpdateTime = timeStr
	mjshbean.Enable = "1"

	bt2 := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_SLAVE_HEART)
	defer bt2.Close()
	fb0 := bt2.Get(mjshbean.SlaveId)
	fb := new(module.MetaJobSlaveHeartBean)
	err = json.Unmarshal([]byte(fb0.(string)), &fb)
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("parse json error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("parse json error.%v", err), nil)
		return
	}
	fb.CurrentCnt = mjshbean.CurrentCnt
	jsonstr, _ := json.Marshal(fb)
	err = bt2.Set(fb.SlaveId, string(jsonstr))
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("data in db update error.%v", err))
		util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("data in db update error.%v", err), nil)
		return
	}
	util.ApiResponse(response.ResponseWriter, 200, "", nil)
}


func (rrs *ResponseResource) SlaveExecCntHandler(request *restful.Request, response *restful.Response) {
	bt := db.NewBoltDB(conf.BboltDBPath+"/"+util.SYS_FL_CONF_STORE, util.SYS_TB_SLAVE_HEART)
	defer bt.Close()

	strlist := bt.Scan()
	//bt.Close()

	retlst := make([]interface{}, 0)
	for _, v := range strlist {
		for k1, v1 := range v.(map[string]interface{}) {
			mjshb := new(module.MetaJobSlaveHeartBean)
			err := json.Unmarshal([]byte(v1.(string)), &mjshb)
			if err != nil {
				glog.Glog(LogF, fmt.Sprint(err))
				continue
			}
			timeStr := time.Now().Format("2006-01-02 15:04:05")
			loc, _ := time.LoadLocation("Local")
			timeLayout := "2006-01-02 15:04:05"
			stheTime, _ := time.ParseInLocation(timeLayout, mjshb.UpdateTime, loc)
			sst := stheTime.Unix()
			etheTime, _ := time.ParseInLocation(timeLayout, timeStr, loc)
			est := etheTime.Unix()
			if est-sst > 600 {
				glog.Glog(LogF, fmt.Sprintf("%v, %v:%v heart timeout.", mjshb.SlaveId, mjshb.Ip, mjshb.Port))
                                _ = bt.Remove(k1)
				continue
			}
			maxcnt, err := strconv.Atoi(mjshb.MaxCnt)
			if err != nil {
				glog.Glog(LogF, fmt.Sprintf("conv maxcnt fail.%v",err))
				continue
			}
			runningcnt, err := strconv.Atoi(mjshb.RunningCnt)
			if err != nil {
				glog.Glog(LogF, fmt.Sprintf("conv runningcnt fail.%v",err))
				continue
			}
			currentcnt, err := strconv.Atoi(mjshb.CurrentCnt)
			if err != nil {
				glog.Glog(LogF, fmt.Sprintf("conv currentcnt fail.%v",err))
				continue
			}
			if maxcnt <= runningcnt+currentcnt {
				glog.Glog(LogF, "max cnt gt running job cnt.")
				continue
			}
			retlst = append(retlst, mjshb)
		}
	}
        bt.Close()
	util.ApiResponse(response.ResponseWriter, 200, "", retlst)
}

func (rrs *ResponseResource) jobListHandleRouter(request *restful.Request, response *restful.Response) {
        reqParams, err := url.ParseQuery(request.Request.URL.RawQuery)
        if err != nil {
                glog.Glog(LogF, fmt.Sprintf("parse para error.%v", err))
                util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("parse para error.%v", err), nil)
                return
        }
        mjwb := new(module.MetaJobWorkerBean)
        err = request.ReadEntity(&mjwb)
        if err != nil {
                glog.Glog(LogF, fmt.Sprint(err))
                util.ApiResponse(response.ResponseWriter, 700, fmt.Sprintf("Parse json error.%v", err), nil)
                return
        }
        tipb, _ := getFlowListener(mjwb.FlowId)
        if tipb == nil {
                glog.Glog(LogF, "flow "+mjwb.FlowId+" has stopped.")
                util.ApiResponse(response.ResponseWriter, 700, "flow "+mjwb.FlowId+" has stopped.", nil)
                return
        }
        url1 := fmt.Sprintf("http://%v:%v/mst/job/ls?accesstoken=%v&flowid=%v",tipb.Ip,tipb.Port,fmt.Sprint(reqParams["accesstoken"][0]),mjwb.FlowId)
        request.Request.Header.Add("Content-Type", "application/x-www-form-urlencoded")
        request.Request.Header.Add("Access-Control-Allow-Origin", "*")
        http.Redirect(response.ResponseWriter, request.Request, url1, http.StatusFound)
        return
}

var (
	cfg     = flag.String("conf", "conf.yaml", "basic config")
	conf    *module.MetaApiServerBean
	jobpool = db.NewMemDB()
	slvMap  = db.NewMemDB()
	LogF    string
	//mstMap  map[string]interface{}
	mstMap = db.NewMemDB()
)

func main() {
	flag.Parse()
	conf = new(module.MetaApiServerBean)
	yamlFile, err := ioutil.ReadFile(*cfg)
	if err != nil {
		log.Printf("error: %s", err)
		return
	}
	err = yaml.UnmarshalStrict(yamlFile, conf)
	if err != nil {
		log.Printf("error: %s", err)
		return
	}
	LogF = conf.HomeDir + "/handle_${" + util.ENV_VAR_DATE + "}.log"
	//mstMap = make(map[string]interface{})
	for i := conf.MstMinPort; i <= conf.MstMaxPort; i++ {
		portstr := fmt.Sprintf("%v", i)
		tkey := conf.MstIp + ":" + portstr
		//ipb := new(module.MetaInstancePortBean)
                ipb := new(MetaInstancePortBean)
		ipb.Enable = "0"
		ipb.Ip = conf.MstIp
		ipb.Port = portstr
		//mstMap[tkey] = ipb
		mstMap.Add(tkey, ipb)
	}
	//var waitGroup util.WaitGroupWrapper
	//waitGroup.Wrap(func() { HttpServer() })
	go func() {
		JobPool()
	}()
	HttpServer()
}

func JobPool() {
	for {
		glog.Glog(LogF, fmt.Sprintf("Check Job Pool..."))
		url := fmt.Sprintf("http://%v:%v/api/v1/job/pool/ls?accesstoken=%v", conf.ApiServerIp, conf.ApiServerPort, conf.AccessToken)
		jsonstr, err := util.Api_RequestPost(url, "{}")
		if err != nil {
			glog.Glog(LogF, fmt.Sprint(err))
			time.Sleep(time.Duration(10) * time.Second)
			continue
		}
		retbn := new(module.RetBean)
		err = json.Unmarshal([]byte(jsonstr), &retbn)
		if err != nil {
			glog.Glog(LogF, fmt.Sprint(err))
			time.Sleep(time.Duration(10) * time.Second)
			continue
		}
		if retbn.Status_Code != 200 {
			glog.Glog(LogF, fmt.Sprintf("post url return status code:%v", retbn.Status_Code))
			time.Sleep(time.Duration(10) * time.Second)
			continue
		}
		if retbn.Data == nil {
			glog.Glog(LogF, fmt.Sprint("get pending status job err."))
			time.Sleep(time.Duration(10) * time.Second)
			continue
		}
		retarr := (retbn.Data).([]interface{})
		serverlst := ObtSlvRunningJobCnt()
		for i := 0; i < len(retarr); i++ {
			v := retarr[i].(map[string]interface{})
			s := ObtJobServer(serverlst, v["server"].(string))
			if len(s) < 1 {
				glog.Glog(LogF, fmt.Sprintf("%v, %v, %v job no free server running,wait for next time.", v["flowid"], v["sys"], v["job"]))
				time.Sleep(time.Duration(10) * time.Second)
				continue
			}
			SubmitJob(v, s[0])
		}
		if len(retarr) == 0 {
			glog.Glog(LogF, fmt.Sprint("job pool empty ,no running job ."))
		} else {
			glog.Glog(LogF, fmt.Sprintf("do with job cnt %v", len(retarr)))
		}
		time.Sleep(time.Duration(10) * time.Second)
	}
}

func SubmitJob(jobstr interface{}, slvstr interface{}) bool {
	s := slvstr.(map[string]interface{})
	jp := jobstr.(map[string]interface{})
	flg := 0
	for k := range mstMap.MemMap {
		//masb := (mstMap.MemMap[k]).(*module.MetaInstancePortBean)
                masb := (mstMap.MemMap[k]).(*MetaInstancePortBean)
		if masb.FlowId == jp["flowid"].(string) {
			flg = 2
			if masb.Enable != "1" {
				glog.Glog(LogF, fmt.Sprintf("%v instance port enable !=1.", masb.FlowId))
				return false
			}
			glog.Glog(LogF, fmt.Sprintf("update %v,%v,%v job status %v.", jp["sys"], jp["job"], jp["flowid"], util.SYS_STATUS_GO))
			url0 := fmt.Sprintf("http://%v:%v/mst/job/status/update/go?accesstoken=%v&sys=%v&job=%v&server=%v&status=%v&ip=%v&port=%v", masb.Ip, masb.Port, conf.AccessToken, jp["sys"], jp["job"], s["slaveid"], util.SYS_STATUS_GO,s["ip"],s["port"])
			//jpb := fmt.Sprintf("{\"sys\":\"%v\",\"job\":\"%v\",\"server\":\"%v\"}", jp["sys"], jp["job"], s["slaveid"])
			jsonstr, err := util.Api_RequestPost(url0, "{}")
			if err != nil {
				glog.Glog(LogF, fmt.Sprint(err))
				return false
			}
			retbn := new(module.RetBean)
			err = json.Unmarshal([]byte(jsonstr), &retbn)
			if err != nil {
				glog.Glog(LogF, fmt.Sprint(err))
				return false
			}
			if retbn.Status_Code != 200 {
				glog.Glog(LogF, fmt.Sprintf("post url %v return status code:%v,msg: %v", url0, retbn.Status_Code, retbn.Status_Txt))
				return false
			}
			flg = 1
			break
		}
	}
	if flg != 1 {
		if flg == 2 {
			glog.Glog(LogF, fmt.Sprintf("update %v, %v, %v status fail.", jp["flowid"], jp["sys"], jp["job"]))
		} else if flg == 0 {
			glog.Glog(LogF, fmt.Sprintf("%v instance has stopped.", jp["flowid"]))
		}
		return false
	}
	glog.Glog(LogF, fmt.Sprintf("rm from job pool %v,%v,%v.", jp["sys"], jp["job"], jp["flowid"]))
	url := fmt.Sprintf("http://%v:%v/api/v1/job/pool/rm?accesstoken=%v", conf.ApiServerIp, conf.ApiServerPort, conf.AccessToken)
	jobp := fmt.Sprintf("{\"sys\":\"%v\",\"job\":\"%v\",\"flowid\":\"%v\"}", jp["sys"], jp["job"], jp["flowid"])
	jsonstr0, err := util.Api_RequestPost(url, jobp)
	if err != nil {
		glog.Glog(LogF, fmt.Sprint(err))
		return false
	}
	retbn1 := new(module.RetBean)
	err = json.Unmarshal([]byte(jsonstr0), &retbn1)
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

func ObtSlvRunningJobCnt() []interface{} {
	url := fmt.Sprintf("http://%v:%v/api/v1/slave/cnt/exec?accesstoken=%v", conf.ApiServerIp, conf.ApiServerPort, conf.AccessToken)
	jsonstr, err := util.Api_RequestPost(url, "{}")
	if err != nil {
		glog.Glog(LogF, fmt.Sprint(err))
		return nil
	}
	retbn := new(module.RetBean)
	err = json.Unmarshal([]byte(jsonstr), &retbn)
	if err != nil {
		glog.Glog(LogF, fmt.Sprint(err))
		return nil
	}
	if retbn.Status_Code != 200 {
		glog.Glog(LogF, fmt.Sprintf("post url return status code:%v", retbn.Status_Code))
		return nil
	}
	if retbn.Data == nil {
		glog.Glog(LogF, fmt.Sprint("get pending status job err."))
		return nil
	}
	retarr := (retbn.Data).([]interface{})
	return retarr
}

func ObtJobServer(arr []interface{}, slvid string) []interface{} {
	retlst := make([]interface{}, 0)
	minv := 0
	var server interface{}
	for i := 0; i < len(arr); i++ {
		v := arr[i].(map[string]interface{})
		if len(slvid) > 0 {
			if v["slaveid"] == slvid {
				retlst = append(retlst, arr[i])
				break
			}
			continue
		} else {
			maxcnt, err := strconv.Atoi(v["maxcnt"].(string))
			if err != nil {
				glog.Glog(LogF, fmt.Sprintf("string conv int %v err.%v",v["maxcnt"], err))
				continue
			}
			runningcnt, err := strconv.Atoi(v["runningcnt"].(string))
			if err != nil {
				glog.Glog(LogF, fmt.Sprintf("string conv int %v err.%v",v["runningcnt"], err))
				continue
			}
			currentcnt, err := strconv.Atoi(v["currentcnt"].(string))
			if err != nil {
				glog.Glog(LogF, fmt.Sprintf("string conv int %v err.%v",v["currentcnt"] ,err))
				continue
			}
			if minv < (maxcnt - runningcnt - currentcnt) {
				minv = maxcnt - runningcnt - currentcnt
				server = arr[i]
			}
		}
	}
	if minv != 0 {
		retlst = append(retlst, server)
	}
	return retlst
}

func HttpServer() {
        restful.Filter(globalOauth)

	// Optionally, you may need to enable CORS for the UI to work.
	cors := restful.CrossOriginResourceSharing{
		ExposeHeaders:  []string{"X-My-Header"},
		AllowedHeaders: []string{"Content-Type", "Accept", "Access-Control-Allow-Headers", "Access-Control-Allow-Origin","Access-Control-Allow-*"},
		AllowedMethods: []string{"GET", "POST", "PUT", "DELETE"},
		CookiesAllowed: false,
		Container:      restful.DefaultContainer}
	restful.DefaultContainer.Filter(cors.Filter)

	rrs := ResponseResource{Data: make(map[string]interface{})}
	restful.DefaultContainer.Add(rrs.WebService())

	config := restfulspec.Config{
		WebServices:                   restful.RegisteredWebServices(), // you control what services are visible
		APIPath:                       "/apidocs.json",
		PostBuildSwaggerObjectHandler: enrichSwaggerObject}
	restful.DefaultContainer.Add(restfulspec.NewOpenAPIService(config))

	// Optionally, you can install the Swagger Service which provides a nice Web UI on your REST API
	// You need to download the Swagger HTML5 assets and change the FilePath location in the config below.
	// Open http://localhost:8080/apidocs/?url=http://localhost:8080/apidocs.json
	http.Handle("/apidocs/", http.StripPrefix("/apidocs/", http.FileServer(http.Dir("/home/k8s/Go/ws/src/github.com/lhzd863/autoflow/swagger-ui-3.22.0"))))

	log.Printf("Get the API using http://localhost:" + conf.Port + "/apidocs.json")
	log.Printf("Open Swagger UI using http://"+conf.ApiServerIp+":" + conf.Port + "/apidocs/?url=http://localhost:" + conf.Port + "/apidocs.json")
	log.Fatal(http.ListenAndServe(":"+conf.Port, nil))
}

func enrichSwaggerObject(swo *spec.Swagger) {
	swo.Info = &spec.Info{
		InfoProps: spec.InfoProps{
			Title:       "Autoflow",
			Description: "Resource for managing Api",
			Contact: &spec.ContactInfo{
				Name:  "lhzd863",
				Email: "lhzd863@126.com",
				URL:   "http://lhzd863.com",
			},
			License: &spec.License{
				Name: "MIT",
			        URL:  "http://lhzd863.com",
			},
			Version: "1.0.0",
		},
	}
	swo.Tags = []spec.Tag{spec.Tag{TagProps: spec.TagProps{
		Name:        "image",
		Description: "Managing image"}}}
}

// Global Filter
func globalOauth(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
	u, err := url.Parse(req.Request.URL.String())
	if err != nil {
		log.Println("parse url error")
		return
	}
	if u.Path == "/api/login" || u.Path == "/api/register"|| u.Path == "/apidocs"||u.Path == "/apidocs.json" {
		chain.ProcessFilter(req, resp)
		return
	}
	reqParams, err := url.ParseQuery(req.Request.URL.RawQuery)
	if err != nil {
		log.Printf("Failed to decode: %v", err)
		util.ApiResponse(resp.ResponseWriter, 700, fmt.Sprintf("Failed decode error.%v", err), nil)
		return
	}
	tokenstring := fmt.Sprint(reqParams["accesstoken"][0])
	var claimsDecoded map[string]interface{}
	decodeErr := jwt.Decode([]byte(tokenstring), &claimsDecoded, []byte(conf.JwtKey))
	if decodeErr != nil {
		log.Printf("Failed to decode: %s (%s)", decodeErr, tokenstring)
		util.ApiResponse(resp.ResponseWriter, 700, fmt.Sprintf("Failed to decode.$v", decodeErr), nil)
		return
	}

	exp := claimsDecoded["exp"].(float64)
	exp1, _ := strconv.ParseFloat(fmt.Sprintf("%v", time.Now().Unix()+0), 64)

	if (exp - exp1) < 0 {
		log.Printf("Failed to decode: %v %v %v", exp, exp1, (exp - exp1))
		util.ApiResponse(resp.ResponseWriter, 700, "Not Authorized AccessToken Expired ,Please login", nil)
		return
	}
	//fmt.Println((exp - exp1))
	chain.ProcessFilter(req, resp)
}
