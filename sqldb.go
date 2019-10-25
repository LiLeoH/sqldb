package sqldb

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"reflect"
	"revenuemesh/pkg/yylog"
	"strings"
	"sync"
	"time"
)

// DbConfig json config example:
// {
// 	"sqldb": [
// 		{
// 			"type": "mysql",
// 			"tag": "t1",
// 			"dbname": "db0",
// 			"ip": "1.1.1.1",
// 			"port": 1234,
// 			"username": "yy",
// 			"password": "123456",
// 			"timeout": 2000,
// 			"max_open_conns": 10
//			"max_idle_conns": 5
// 		},
// 		{
// 			"type": "mysql",
// 			"tag": "t2",
// 			"dbname": "db1",
// 			"ip": "2.2.2.2",
// 			"port": 5678,
// 			"username": "yy",
// 			"password": "123456",
// 			"timeout": 5000,
// 			"max_open_conns": 20
//			"max_idle_conns": 5
// 		}
// 	]
// }
//
type DbConfig struct {
	DbType       string `json:"type"`
	Tag          string `json:"tag"`
	DbName       string `json:"dbname"`
	IP           string `json:"ip"`
	Port         int    `json:"port"`
	Username     string `json:"username"`
	Password     string `json:"password"`
	Charset      string `json:"charset"`
	Timeout      int    `json:"timeout"`
	MaxOpenConns int    `json:"max_open_conns"`
	MaxIdleConns int    `json:"max_idle_conns"`
}

type DbConfigWrapper struct {
	Sqldb []DbConfig `json:"sqldb"`
}

type mgr struct {
	clients sync.Map
}

// struct field tag associated with database table column name
var sqlColumnTag = "sql"

func NewSqlDBMgr() (*mgr, error) {
	m := &mgr{}

	return m, nil
}

// get one row data from sql.Rows
// `dest` must be *struct with tag using `sqlColumnTag`
func GetRowData(rows *sql.Rows, dest interface{}) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	var rowInfo = make([]interface{}, len(columns))
	type refIndex struct { //support nested struct
		i, j int
	}
	var colDestMap = make(map[refIndex]int)

	var isMatch = func(colName string, i, j, n int, sf *reflect.StructField) bool {
		ts := strings.Split(sf.Tag.Get(sqlColumnTag), ",")
		if colName == ts[0] || colName == sf.Name {
			colDestMap[refIndex{i, j}] = n
			rowInfo[n] = reflect.New(sf.Type).Interface()
			return true
		}
		return false
	}

	desType := reflect.TypeOf(dest).Elem()
	for n, colName := range columns {
		matched := false
		for i := 0; !matched && i < desType.NumField(); i++ {
			sf := desType.Field(i)
			if sf.Type.Kind() == reflect.Struct {
				if _, ok := sf.Tag.Lookup(sqlColumnTag); ok {
					matched = isMatch(colName, i, -1, n, &sf)
				} else {
					for j := 0; !matched && j < sf.Type.NumField(); j++ {
						sf2 := sf.Type.Field(j)
						matched = isMatch(colName, i, j, n, &sf2)
					}
				}
			} else {
				matched = isMatch(colName, i, -1, n, &sf)
			}
		}
	}

	err = rows.Scan(rowInfo...)
	if err != nil {
		return err
	}

	destValue := reflect.ValueOf(dest).Elem()
	for idx, n := range colDestMap {
		if idx.j < 0 {
			destValue.Field(idx.i).Set(reflect.ValueOf(rowInfo[n]).Elem())
		} else {
			destValue.FieldByIndex([]int{idx.i, idx.j}).Set(reflect.ValueOf(rowInfo[n]).Elem())
		}
	}

	return nil
}

// query data from db
// `dest` pass *struct for one row data
// `dest` pass *Slice for multi-line data
func GetDataFromDb(db *sql.DB, dest interface{}, sqlCmd string, args ...interface{}) error {
	rows, err := db.Query(sqlCmd, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	desType := reflect.TypeOf(dest).Elem()
	switch desType.Kind() {
	case reflect.Slice:
		destValue := reflect.ValueOf(dest).Elem()
		for rows.Next() {
			rowData := reflect.New(desType.Elem())
			err = GetRowData(rows, rowData.Interface())
			if err != nil {
				return err
			}
			destValue.Set(reflect.Append(destValue, rowData.Elem()))
		}
	case reflect.Struct:
		if rows.Next() {
			return GetRowData(rows, dest)
		}
	default:
		return errors.New("Type Error: expected Slice or Struct!")
	}

	return nil
}

func (s *mgr) Init(cfgs []DbConfig, dailer func(addr string) (net.Conn, error)) (err error) {
	for _, cfg := range cfgs {
		dsn, err := getDataSourceName(&cfg)
		if err != nil {
			return err
		}
		yylog.LogF("info", "sqldb.Init tag:%s dsn: %s", cfg.Tag, dsn)

		if dailer != nil {
			err = RegistDialer(dailer)
			if err != nil {
				return err
			}
		}

		db, err := sql.Open(cfg.DbType, dsn)
		if err != nil {
			return err
		}

		if cfg.MaxOpenConns > 0 {
			db.SetMaxOpenConns(cfg.MaxOpenConns)
		}

		//@see https://github.com/go-sql-driver/mysql/issues/674
		db.SetMaxIdleConns(cfg.MaxIdleConns)
		db.SetConnMaxLifetime(5 * time.Minute)

		s.clients.Store(cfg.Tag, db)
	}

	return nil
}

// Destory close sql.db
func (s *mgr) Destory() {
	s.clients.Range(func(k, v interface{}) bool {
		db := v.(*sql.DB)
		db.Close()
		return true
	})
}

// Query wrapper function of sql.DB.Query using tag
func (s *mgr) Query(dest interface{}, tag string, sqlCmd string, args ...interface{}) (err error) {
	if val, ok := s.clients.Load(tag); ok {
		db := val.(*sql.DB)
		return GetDataFromDb(db, dest, sqlCmd, args...)
	}

	return fmt.Errorf("Cannot find db with tag[%s]", tag)
}

// Exec wrapper function of sql.DB.Exec using tag
func (s *mgr) Exec(tag, sqlCmd string, args ...interface{}) (res sql.Result, err error) {
	if val, ok := s.clients.Load(tag); ok {
		db := val.(*sql.DB)
		return db.Exec(sqlCmd, args...)
	}

	return nil, fmt.Errorf("Cannot find db with tag[%s]", tag)
}

// GetDB get *sql.DB of specified tag
func (s *mgr) GetDB(tag string) *sql.DB {
	if val, ok := s.clients.Load(tag); ok {
		return val.(*sql.DB)
	}
	return nil
}
