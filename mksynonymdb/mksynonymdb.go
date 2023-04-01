package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func getTables(db *sql.DB, schema string) ([]string, error) {
	stmt, err := db.Prepare("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA=?")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}
	return tables, nil
}

func createViews(db *sql.DB, target string, name string, tables []string) error {
	for _, table := range tables {
		log.Printf("Creating view for %s\n", table)
		stmt := fmt.Sprintf("CREATE VIEW `%s`.`%s` AS SELECT * FROM `%s`.`%s`\n", name, table, target, table)
		_, err := db.Exec(stmt)
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	var dbUri = flag.String("uri", "root@tcp(127.0.0.1:4000)/", "database URI")
	var target = flag.String("target", "", "synonym target")
	var name = flag.String("name", "", "synonym name")
	flag.Parse()

	if *target == "" {
		log.Fatal("Please supply a target name")
	}

	if *name == "" {
		log.Fatal("Please supply a synonym name")
	}

	db, err := sql.Open("mysql", *dbUri)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("CREATE SCHEMA " + *name)
	if err != nil {
		log.Fatal(err)
	}

	tables, err := getTables(db, *target)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Tables to create synonyms for: %s", strings.Join(tables[:], ","))

	err = createViews(db, *target, *name, tables)
	if err != nil {
		log.Fatal(err)
	}
}
