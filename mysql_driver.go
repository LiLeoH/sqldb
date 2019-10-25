package sqldb

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/go-sql-driver/mysql"
)

func RegistDialer(dailer func(addr string) (net.Conn, error)) error {
	mysql.RegisterDial("tcp", dailer)

	return nil
}

func getDataSourceName(cfg *DbConfig) (string, error) {
	if cfg.DbType != "mysql" {
		return "", errors.New("expect db type [mysql]")
	}
	var dsnFormat = "%s:%s@(%s:%d)/%s?%s"

	var params []string
	if len(cfg.Charset) == 0 {
		params = append(params, "charset=utf8mb4")
	} else {
		params = append(params, fmt.Sprintf("charset=%s", cfg.Charset))
	}

	if cfg.Timeout < 0 {
		params = append(params, "timeout=5s")
	} else {
		params = append(params, fmt.Sprintf("timeout=%dms", cfg.Timeout))
	}

	params = append(params, fmt.Sprintf("loc=%s", url.QueryEscape("Asia/Shanghai")))
	params = append(params, "parseTime=true")
	params = append(params, "interpolateParams=true")

	paramStr := strings.Join(params, "&")

	return fmt.Sprintf(dsnFormat, cfg.Username, cfg.Password, cfg.IP, cfg.Port, cfg.DbName, paramStr), nil
}

func GetMysqlErrorNumber(err interface{}) (uint16, error) {
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return 1, fmt.Errorf("expected *mysql.MySQLError, got %T", err)
	}

	if mysqlErr == nil {
		return 0, nil
	}
	return mysqlErr.Number, nil
}

func GetMysqlErrorMessage(err interface{}) (string, error) {
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return "", fmt.Errorf("expected *mysql.MySQLError, got %T", err)
	}

	if mysqlErr == nil {
		return "ok", nil
	}
	return mysqlErr.Message, nil
}

func GetMysqlErrorInfo(err interface{}) (uint16, string, error) {
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return 1, "", fmt.Errorf("expected *mysql.MySQLError, got %T", err)
	}

	if mysqlErr == nil {
		return 0, "ok", nil
	}

	return mysqlErr.Number, mysqlErr.Message, nil
}
