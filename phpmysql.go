package phpmysql

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

type Mysql struct {
	Host     string
	User     string
	Password string
	Database string
	link     *sql.DB
}

func (t *Mysql) Connect() {
	db, err := sql.Open("mysql", t.User+":"+t.Password+"@tcp("+t.Host+")/"+t.Database+"?charset=utf8")
	if err != nil {
		defer db.Close()
	}
	t.link = db
}
func (t *Mysql) Close() {
	t.link.Close()
}

func (t *Mysql) Query(sql string) (results [](map[string]string), count int) {

	rows, err := t.link.Query(sql)
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

func (t *Mysql) One(sql string) (result map[string]string, has bool) {

	rows, err := t.link.Query(sql)
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
