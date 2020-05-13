package db

import (
	"log"
	"sync"
)

type MemDB struct {
	sync.RWMutex
	MemMap map[string]interface{}
}

func NewMemDB() *MemDB {
        return &MemDB{
                MemMap: make(map[string]interface{}),
        }
}

func (r *MemDB) Add(k string, v interface{}) {
	r.Lock()
	defer r.Unlock()
	_, ok := r.MemMap[k]
	if !ok {
		r.MemMap[k] = v
	}
}

func (r *MemDB) Remove(k string) {
	r.Lock()
	defer r.Unlock()
	delete(r.MemMap, k)
}

func (r *MemDB) Update(k string, v interface{}) {
	r.Lock()
	defer r.Unlock()
	delete(r.MemMap, k)
	r.MemMap[k] = v
}

func (r *MemDB) Get(k string) interface{} {
	r.Lock()
	defer r.Unlock()
	v, ok := r.MemMap[k]
	if !ok {
		log.Println(ok)
	}
	return v
}
