package util

const (
	BINARY_VERSION = "v0.0.1"
	ENV_VAR_DATE   = "YYYYMMDD"
	//ctl
	JOB_CTL_ID        = "job.ctl.id"
	JOB_CTL_SYS       = "job.ctl.sys"
	JOB_CTL_NAME      = "job.ctl.name"
	JOB_CTL_DATE      = "job.ctl.date"
	JOB_CTL_TIME      = "job.ctl.time"
	JOB_CTL_TIMESTAMP = "job.ctl.timestamp"
	//job conf
	JOB_CONF_RETRYCNT           = "job.conf.retrycnt"
	JOB_CONF_JOBTYPE            = "job.conf.jobtype"
	JOB_CONF_ALARMTYPE          = "job.conf.alarmtype"
	JOB_CONF_RUNNING_SCRIPTNAME = "job.conf.running.scriptname"
	JOB_CONF_LOGID              = "job.conf.logid"
	//
	JOB_STREAM_SYS = "job.stream.sys"
	JOB_STREAM_JOB = "job.stream.job"
	//status
	JOB_STATUS_SUBMIT  = "job.status.submit"
	JOB_STATUS_RUNNING = "job.status.running"
	JOB_STATUS_SUCC    = "job.status.succ"
	JOB_STATUS_FAIL    = "job.status.fail"
	JOB_STATUS_READY   = "job.status.ready"
	JOB_STATUS_PENDING = "job.status.pending"
	JOB_STATUS_CURRENT = "job.status.current"
	//
	SLV_NODE_NAME         = "slv.node.name"
	SLV_NODE_MAX_PARALLEL = "slv.node.max.parallel"
	SLV_NODE_SLEEP_TIME   = "slv.node.sleep.time"
	//
	BATCH_HOME = "batch.home"
	BATCH_LOG  = "batch.log"
	//
	SLV_SLEEP_TIME = "slv.sleep.time"
        MST_SLEEP_TIME = "mst.sleep.time"
)
