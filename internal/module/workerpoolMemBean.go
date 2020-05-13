package module

import (
	"github.com/lhzd863/autoflow/internal/workpool"
)

type MetaWorkerPoolMemBean struct {
	Id         string             `json:"id"`
	WorkerPool *workpool.WorkPool `json:"workerpool"`
	CreateTime string             `json:"cts"`
	Enable     string             `json:"enable"`
}
