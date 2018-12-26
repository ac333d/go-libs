package influx

import (
	"fmt"
	"time"

	client "github.com/influxdata/influxdb/client/v2"
)

// Client - Maintains database session type
type Client = client.Client

// InitUDP - InitUDP
func InitUDP(host string, port int) (Client, error) {
	config := client.UDPConfig{Addr: fmt.Sprintf("%s:%d", host, port)}
	c, err := client.NewUDPClient(config)
	if err != nil {
		return nil, err
	}
	return Client(c), nil
}

// InitHTTP - InitHTTP
func InitHTTP(host string, port int, username string, password string) (Client, error) {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     fmt.Sprintf("http://%s:%d", host, port),
		Username: username,
		Password: password,
	})
	if err != nil {
		return nil, err
	}
	return Client(c), nil
}

// Query - Query
func Query(c Client, dbname, cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: dbname,
	}
	if response, err := c.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}

// CreateDB - CreateDB
func CreateDB(c Client, dbname string) error {
	_, err := Query(c, dbname, fmt.Sprintf("CREATE DATABASE %s", dbname))
	return err
}

// UseDB - UseDB
func UseDB(c Client, dbname string) error {
	_, err := Query(c, dbname, fmt.Sprintf("USE %s", dbname))
	return err
}

// CreateUser - CreateUser
func CreateUser(c Client, username, password, dbname string) error {
	_, err := Query(c, dbname, fmt.Sprintf("CREATE user %s with password '%s'", username, password))
	return err
}

// CreateSuperUser - CreateSuperUser
func CreateSuperUser(c Client, username, password, dbname string) error {
	_, err := Query(c, dbname, fmt.Sprintf("CREATE user %s with password '%s'", username, password))
	if err != nil {
		return err
	}
	_, err = Query(c, dbname, fmt.Sprintf("GRANT all privileges to %s", username))
	return err
}

// CountFields - CountFields
func CountFields(c Client, dbname, value, measurement string) (res []client.Result, err error) {
	p, err := Query(c, dbname, fmt.Sprintf("select count(%s) from %s", value, measurement))
	if err != nil {
		return nil, err
	}
	return p, nil
}

// GetByField - GetByField
func GetByField(c Client, dbname, field string) (res []client.Result, err error) {
	p, err := Query(c, dbname, fmt.Sprintf("select * from \"%s\"..\"%s\"", dbname, field))
	if err != nil {
		return nil, err
	}
	return p, nil
}

// InsertBatchWithTime - InsertBatchWithTime
func InsertBatchWithTime(c Client, dbname, measurement, tag, tagName string, fields map[string]interface{}, timestamp time.Time) error {
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  dbname,
		Precision: "us",
	})
	tags := map[string]string{tag: tagName}
	field := fields
	pt, err := client.NewPoint(measurement, tags, field, timestamp)
	if err != nil {
		return err
	}
	bp.AddPoint(pt)
	return c.Write(bp)
}

// InsertBatch - InsertBatch
func InsertBatch(c Client, dbname, measurement, tag, tagName string, fields map[string]interface{}, timestamp time.Time) error {
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  dbname,
		Precision: "us",
	})
	tags := map[string]string{tag: tagName}
	field := fields
	pt, err := client.NewPoint(measurement, tags, field, time.Now())
	if err != nil {
		return err
	}
	bp.AddPoint(pt)
	return c.Write(bp)
}

// Insert - Insert
func Insert(c Client, dbname, measurement, key, value string) error {
	_, err := Query(c, dbname, fmt.Sprintf("INSERT %s %s=%s", measurement, key, value))
	return err
}

// DeleteAll - DeleteAll
func DeleteAll(c Client, dbname, measurement string) error {
	_, err := Query(c, dbname, fmt.Sprintf("DELETE FROM \"%s\"", measurement))
	return err
}

// CloseClient - CloseClient
func CloseClient(c Client) error {
	return c.Close()
}
