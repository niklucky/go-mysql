package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"log"

	"strings"

	_ "github.com/go-sql-driver/mysql" // mysql
)

/*
Mapper - MySQL mapper DAO
*/
type Mapper struct {
	DBConfig        DBConfig
	Conn            *sql.DB
	Source          string
	Logger          Logger
	BuildCollection func(*sql.Rows) ([]interface{}, error)
}

/*
DBConfig - Postgres config
*/
type DBConfig struct {
	User,
	Password,
	Host,
	Port,
	Database,
	SSLmode string
}

/*
Logger — interface to log
*/
type Logger interface {
	Log(...interface{}) error
	Error(...interface{}) error
	Fatal(...interface{}) error
}

/*
New - Mapper constructor
*/
func New(config DBConfig) Mapper {
	return Mapper{
		DBConfig: config,
	}
}

/*
Connect - connecting to DB
*/
func (mapper *Mapper) Connect() error {
	dbConfig := mapper.DBConfig
	connectionString := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v",
		dbConfig.User,
		dbConfig.Password,
		dbConfig.Host,
		dbConfig.Port,
		dbConfig.Database,
	)
	mapper.log("Connecting to mysql: ", mapper.getDbInfo())
	conn, err := sql.Open("mysql", connectionString)
	if err != nil {
		return err
	}
	if conn == nil {
		return errors.New("Connection to MySQL is nil")
	}
	mapper.log("Connected to mysql: ", mapper.getDbInfo())
	mapper.Conn = conn
	return nil
}

/*
Exec - executing query
*/
func (mapper *Mapper) Exec(query string) (*sql.Rows, error) {
	return mapper.Query(query)
}

/*
Query - executing query (same as Exec)
*/
func (mapper *Mapper) Query(query string) (*sql.Rows, error) {
	if mapper.Conn == nil {
		err := mapper.Connect()
		if err != nil {
			return nil, err
		}
	}
	return mapper.Conn.Query(query)
}

func (mapper *Mapper) InsertBatch(fields []string, rows []interface{}, onDuplicate interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	if mapper.Conn == nil {
		mapper.Connect()
	}
	var values = []interface{}{}
	SQL := "insert into " + mapper.Source + " (" + strings.Join(fields, ",") + ") values "
	var pl []string
	var placeholder []string

	for n := 0; n < len(fields); n++ {
		pl = append(pl, "?")
	}

	for _, row := range rows {
		r := row.([]interface{})
		for i := 0; i < len(r); i++ {
			values = append(values, r[i])
		}
		placeholder = append(placeholder, "("+strings.Join(pl, ",")+")")
	}
	SQL += strings.Join(placeholder, ",")
	// SQL = SQL[0 : len(SQL)-1]
	if onDuplicate != nil {
		SQL += " ON DUPLICATE KEY UPDATE " + onDuplicate.(string)
	}
	stmt, err := mapper.Conn.Prepare(SQL)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, execErr := stmt.Exec(values...)
	if execErr != nil {
		fmt.Println("MySQL exec: ", execErr)
		return execErr
	}
	return nil
}
func (mapper *Mapper) Insert(fields []string, rows interface{}, onDuplicate interface{}) error {
	var data []interface{}
	data = append(data, rows)
	return mapper.InsertBatch(fields, data, onDuplicate)
}

func (mapper *Mapper) Load(source string, fields string, query interface{}) (*sql.Rows, error) {
	if mapper.Conn == nil {
		mapper.Connect()
	}

	SQL := "SELECT " + fields + " FROM " + source
	if query != nil {
		SQL += " WHERE " + query.(string)
	}
	SQL += ";"
	// fmt.Println(SQL)
	rows, err := mapper.Conn.Query(SQL)
	if err != nil {
		return rows, err
	}
	return rows, nil
}

/*
Close - closing connection
*/
func (mapper *Mapper) Close() error {
	log.Println("Closing connection in mapper")
	if mapper.Conn != nil {
		return mapper.Conn.Close()
	}
	return nil
}

func (mapper *Mapper) log(data ...interface{}) error {
	if mapper.Logger != nil {
		return mapper.Logger.Log(data)
	}
	_, err := fmt.Println(data...)
	return err
}

func (mapper *Mapper) getDbInfo() string {
	c := mapper.DBConfig
	return c.User + "@" + c.Host + ":" + c.Port + "/" + c.Database
}
