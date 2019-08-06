package phpmysql

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type Mysql struct {
	Host     string
	User     string
	Password string
	Database string
	Db       *sql.DB
}

func (t *Mysql) Connect() {
	db, err := sql.Open("mysql", t.User+":"+t.Password+"@tcp("+t.Host+")/"+t.Database+"?charset=utf8")
	if err != nil {
		defer db.Close()
	}
	t.Db = db
}
func (t *Mysql) Close() {
	t.Db.Close()
}

func (t *Mysql) Q(query string, args ...interface{}) (sql.Result, error) {
	return t.Db.Exec(query, args...)
}

func (t *Mysql) GetList(sql string, args ...interface{}) (results [](map[string]string), count int) {

	rows, err := t.Db.Query(sql, args...)
	defer rows.Close()
	if err == nil {
		cols, err := rows.Columns()

		rawResult := make([][]byte, len(cols))
		dest := make([]interface{}, len(cols))
		for i, _ := range rawResult {
			dest[i] = &rawResult[i]
		}
		results = make([](map[string]string), 0)

		for rows.Next() {
			count++
			err = rows.Scan(dest...)
			var result map[string]string
			result = make(map[string]string)
			for i, raw := range rawResult {
				if raw == nil {
					result[cols[i]] = ""
				} else {
					result[cols[i]] = string(raw)
				}
			}
			results = append(results, result)
		}
		_ = err
	}
	return results, count
}

func (t *Mysql) GetOne(sql string, args ...interface{}) (result map[string]string, has bool) {

	rows, err := t.Db.Query(sql, args...)
	defer rows.Close()
	if err == nil {
		cols, err := rows.Columns()

		rawResult := make([][]byte, len(cols))
		dest := make([]interface{}, len(cols))
		for i, _ := range rawResult {
			dest[i] = &rawResult[i]
		}
		result = make(map[string]string)
		if rows.Next() {
			err = rows.Scan(dest...)
			result = make(map[string]string)
			for i, raw := range rawResult {
				if raw == nil {
					result[cols[i]] = ""
				} else {
					result[cols[i]] = string(raw)
				}
			}
			has = true
		}
		_ = err
	}
	return result, has
}

/*
	updatearr := make(map[string]interface{}, 0)
	updatearr["newjine"] = 12
	wherearr := make(map[string]interface{}, 0)
	wherearr["cjid"] = 99999
	G.Mysql.Update("cstock_jilu", updatearr, wherearr)
*/
func (t *Mysql) Update(tablename string, setsql map[string]interface{}, wheresql map[string]interface{}) int64 {
	sql := "UPDATE " + tablename + " SET "
	link1 := ""
	link2 := ""
	value := make([]interface{}, 0)
	for k, v := range setsql {
		sql = sql + link1 + k + "=?"
		link1 = ", "
		value = append(value, v)
	}
	sql += " WHERE "
	for k, v := range wheresql {
		sql = sql + link2 + k + "=?"
		link2 = " AND "
		value = append(value, v)
	}
	stmt, err := t.Db.Prepare(sql)
	if err != nil {
		return -1
	}
	res, err1 := stmt.Exec(value...)
	defer stmt.Close()
	if err1 != nil {
		return -2
	}
	affected, err2 := res.RowsAffected()
	if err2 != nil {
		return -3
	}
	return affected
}

/*
	insertarr := make(map[string]interface{}, 0)
	insertarr["link_id"] = 9876543
	re := G.Mysql.Insert("cstock_jilu", insertarr)
	fmt.Println(re)
*/
func (t *Mysql) Insert(tablename string, insertsql map[string]interface{}) int64 {
	sql := "INSERT INTO " + tablename + " "
	sql1 := ""
	sql2 := ""
	link := ""
	value := make([]interface{}, 0)
	for k, v := range insertsql {
		sql1 = sql1 + link + k
		sql2 = sql2 + link + "?"
		link = ", "
		value = append(value, v)
	}
	sql = sql + "(" + sql1 + ") VALUES (" + sql2 + ")"
	stmt, err := t.Db.Prepare(sql)
	if err != nil {
		fmt.Println(sql)
		return -1
	}

	res, err1 := stmt.Exec(value...)
	defer stmt.Close()
	if err1 != nil {
		return -2
	}
	id, err2 := res.LastInsertId()
	if err2 != nil {
		return -3
	}
	return id
}

/*
	insertarrs := make([]map[string]interface{}, 0)
	for i := 0; i < 100; i++ {
		item := make(map[string]interface{}, 0)
		item["link_id"] = 9876543 + i
		insertarrs = append(insertarrs, item)
	}
	re := G.Mysql.Inserts("cstock_jilu", insertarrs)
	fmt.Println(re)

*/
func (t *Mysql) Inserts(tablename string, insertsql []map[string]interface{}) int64 {
	sql := "INSERT INTO " + tablename + " "
	sql1 := ""
	sql2 := ""
	link := ""
	value := make([]interface{}, 0)
	keys := make([]string, 0)
	if len(insertsql) > 0 {
		insertsql0 := insertsql[0]
		for k, v := range insertsql0 {
			sql1 = sql1 + link + k
			sql2 = sql2 + link + "?"
			link = ", "
			keys = append(keys, k)
			value = append(value, v)
		}
		sql = sql + "(" + sql1 + ") VALUES (" + sql2 + ")"
		if len(insertsql) > 1 {
			sql3 := ""
			for i := 1; i < len(insertsql); i++ {
				link = ""
				sql3 += ",("
				for k := 0; k < len(keys); k++ {
					sql3 = sql3 + link + "?"
					link = ","
					value = append(value, insertsql[i][keys[k]])
				}
				sql3 += ")"
			}
			sql += sql3
		}
	} else {
		return -1 //没有数据
	}
	stmt, err := t.Db.Prepare(sql)
	if err != nil {
		fmt.Println(sql)
		return -2 //sql语句错误
	}

	_, err1 := stmt.Exec(value...)
	defer stmt.Close()
	if err1 != nil {
		return -3 //插入数据有误
	}
	return 1
}

func (t *Mysql) Join(arr []string) string {
	r := "'"
	if len(arr) > 0 {
		r += strings.Join(arr, "','")
	}
	r += "'"
	return r
}
