package db

import (
	"log"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

type BoltDB struct {
	sync.RWMutex
	dbPath string
	dbName string
	btdb   *bolt.DB
}

type MapResult []map[string]interface{}

func NewBoltDB(dbpath string, dbname string) *BoltDB {
	db, err := bolt.Open(dbpath, 0666, &bolt.Options{Timeout: 60 * time.Second})
	if err != nil {
		log.Println(err)
		return nil
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(dbname))
		if err != nil {
			log.Println(err)
			return nil
		}
		return nil
	})
	if err != nil {
		log.Println(err)
		return nil
	}

	boltdb := &BoltDB{
		dbPath: dbpath,
		dbName: dbname,
		btdb:   db,
	}
	return boltdb
}

func (bt *BoltDB) Set(k string, v interface{}) error {
	bt.Lock()
	defer bt.Unlock()

	err := bt.btdb.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bt.dbName))
		err := b.Put([]byte(k), []byte(v.(string)))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (bt *BoltDB) Remove(k string) error {
	bt.Lock()
	defer bt.Unlock()

	err := bt.btdb.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bt.dbName))
		err := b.Delete([]byte(k))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (bt *BoltDB) Get(k string) interface{} {
	var output interface{}
	_ = bt.btdb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bt.dbName))
		v := b.Get([]byte(k))
		if len(string(v)) == 0 {
			output = nil
		} else {
			output = string(v)
		}
		return nil
	})
	return output
}

func (bt *BoltDB) Scan() []interface{} {
	output := make([]interface{}, 0)
	_ = bt.btdb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bt.dbName))
		b.ForEach(func(k, v []byte) error {
			retM := make(map[string]interface{})
			retM[string(k)] = string(v)
			output = append(output, retM)
			return nil
		})
		return nil
	})

	return output
}

func (bt *BoltDB) Path() string {
	return bt.btdb.Path()
}

func (bt *BoltDB) Close() {
	bt.btdb.Close()
}
