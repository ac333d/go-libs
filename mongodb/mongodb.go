package mongodb

import (
	"errors"
	"strconv"
	"time"

	mgo "gopkg.in/mgo.v2"
	bson "gopkg.in/mgo.v2/bson"
)

// Session - Maintains database session type
type Session = *mgo.Session

// Init - Connects to database
func Init(host string, port int, username, password, database string, timeout int) (Session, error) {
	if timeout <= 0 {
		timeout = 5
	}
	dialInfo := &mgo.DialInfo{
		Addrs:    []string{host + ":" + strconv.Itoa(port)},
		Timeout:  time.Duration(timeout) * time.Second,
		Database: database,
		Username: username,
		Password: password,
	}

	s, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		return nil, err
	}
	return Session(s), nil
}

// Insert - inserts object into database
func Insert(s Session, dbname string, collection string, object interface{}) error {
	return s.DB(dbname).C(collection).Insert(object)
}

// FindOne - Find one object from the collection of database
func FindOne(s Session, dbname string, collection string, query map[string]interface{}) (interface{}, error) {
	var object interface{}
	if err := s.DB(dbname).C(collection).Find(query).One(&object); err != nil {
		return object, err
	}
	return object, nil
}

// FindOneSpecifiedField - FindOneSpecifiedField
func FindOneSpecifiedField(s Session, dbname string, collection string, query, fields map[string]interface{}) (interface{}, error) {
	var object []interface{}
	if err := s.DB(dbname).C(collection).Find(query).Select(fields).All(&object); err != nil {
		return object, err
	}
	return object, nil

}

// FindAll - Finds all objects from the collection of database
func FindAll(s Session, dbname string, collection string, query map[string]interface{}, pageNum int, pageSize int) ([]interface{}, error) {
	var object []interface{}
	if err := s.DB(dbname).C(collection).Find(query).Skip((pageNum - 1) * pageSize).Limit(pageSize).All(&object); err != nil {
		return object, err
	}
	return object, nil
}

// Update - Updates object from the collection of database
func Update(s Session, dbname string, collection string, selector map[string]interface{}, updator map[string]interface{}) error {
	return s.DB(dbname).C(collection).Update(selector, updator)
}

// UpdateAll - Updates all documents from the collection of database
func UpdateAll(s Session, dbname string, collection string, selector map[string]interface{}, updator map[string]interface{}) error {
	_, err := s.DB(dbname).C(collection).UpdateAll(selector, updator)
	return err
}

// Count - Counts all the records with the matching parameters
func Count(s Session, dbname string, collection string, query map[string]interface{}) (int, error) {
	return s.DB(dbname).C(collection).Find(query).Count()
}

// PipeOne - Pipeleines all the parameters and returns the projected result of one object
func PipeOne(s Session, dbName string, collection string, pipeline []bson.M) (interface{}, error) {
	var object interface{}
	pipe := s.DB(dbName).C(collection).Pipe(pipeline)
	if err := pipe.One(&object); err != nil {
		return object, err
	}
	return object, nil
}

// PipeAll - Pipes all the document and returns the result
func PipeAll(s Session, dbName string, collection string, pipeline []bson.M) ([]interface{}, error) {
	var object []interface{}
	pipe := s.DB(dbName).C(collection).Pipe(pipeline)
	if err := pipe.All(&object); err != nil {
		return object, err
	}
	return object, nil
}

// FindAllSorted - Finds all objects from the collection of database
func FindAllSorted(s Session, dbname string, collection string, query map[string]interface{}, sortParameters string, pageNum int, pageSize int) ([]interface{}, error) {
	var object []interface{}
	if err := s.DB(dbname).C(collection).Find(query).Sort(sortParameters).Skip((pageNum - 1) * pageSize).Limit(pageSize).All(&object); err != nil {
		return object, err
	}
	return object, nil
}

// DoesDocExist - Checks if the documents exists
func DoesDocExist(s Session, dbname string, collection string, query map[string]interface{}, sortParameters string, pageNum int, pageSize int) error {
	count, err := s.DB(dbname).C(collection).Find(query).Sort(sortParameters).Skip((pageNum - 1) * pageSize).Limit(pageSize).Count()

	if count == 0 && err != nil {
		return nil
	}
	return errors.New("Document does exist")
}

// FindAllWithoutPaging - Finds all objects from the collection of database
func FindAllWithoutPaging(s Session, dbname string, collection string, query map[string]interface{}) ([]interface{}, error) {
	var object []interface{}
	if err := s.DB(dbname).C(collection).Find(query).All(&object); err != nil {
		return object, err
	}
	return object, nil
}

// FindAllSpecificFields - Finds all objects from the collection of database which returns only specific fields
func FindAllSpecificFields(s Session, dbname string, collection string, query, fields map[string]interface{}) ([]interface{}, error) {
	var object []interface{}
	if err := s.DB(dbname).C(collection).Find(query).Select(fields).All(&object); err != nil {
		return object, err
	}
	return object, nil
}

// FindOneSorted - Finds the sorted object from the collection of database
func FindOneSorted(s Session, dbname string, collection string, query map[string]interface{}, sortParameter string) (interface{}, error) {
	var object interface{}
	if err := s.DB(dbname).C(collection).Find(query).Sort(sortParameter).One(&object); err != nil {
		return object, err
	}
	return object, nil
}

// Remove - remove objects from database
func Remove(s Session, dbname string, collection string, query map[string]interface{}) error {
	return s.DB(dbname).C(collection).Remove(query)
}

// RemoveAll - remove all objects from database
func RemoveAll(s Session, dbname string, collection string, query map[string]interface{}) (int, error) {
	i, err := s.DB(dbname).C(collection).RemoveAll(query)
	if err != nil {
		return 0, err
	}
	return i.Removed, nil
}

// Upsert - Upserts object from the collection of database
func Upsert(s Session, dbname string, collection string, selector map[string]interface{}, updator map[string]interface{}) error {
	_, err := s.DB(dbname).C(collection).Upsert(selector, updator)
	return err
}
