// modify from https://gist.github.com/mrnugget/0eda3b2b53a70fa4a894
package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	setupSql = `
CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, user_name TEXT);
CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY, product_name TEXT);
DELETE FROM products;
`
	count = 1000000
)

var r *rand.Rand

func init() {
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
}

func main() {
	var (
		dbName = "./testLock.db"
		_, err = os.OpenFile(dbName, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	)
	if err != nil {
		log.Fatal("could create file", err)
	}
	// db, err := sql.Open("sqlite3", dbName)
	db, err := sql.Open("sqlite3", fmt.Sprintf("%s?_mutex=no", dbName))
	if err != nil {
		log.Fatal("could not open sqlite3 database file", err)
	}
	db.SetMaxIdleConns(50)
	// db.SetConnMaxLifetime(time.Hour / 2)
	db.SetMaxOpenConns(15)
	defer db.Close()
	setup(db)

	mu := &sync.Mutex{}

	go func() {
		// writes to users table
		for i := 0; i < count; i++ {
			write(db, mu, i, count)
			randomSleep()
		}

		// done <- struct{}{}
	}()

	go func() {
		// reads from products table, each read in separate go routine
		for i := 0; i < count; i++ {
			go func(i, count int) {
				read(db, mu, i, count)
			}(i, count)

			go func(i, count int) {
				read(db, mu, i, count)

			}(i, count)
			randomSleep()
		}
	}()
	go func() {
		// reads from products table, each read in separate go routine
		for i := 0; i < count; i++ {
			go func(i, count int) {
				read(db, mu, i, count)
			}(i, count)

			go func(i, count int) {
				read(db, mu, i, count)

			}(i, count)
			randomSleep()
		}
	}()

	// wait goroutine
	time.Sleep(time.Minute * 3)
}

func randomSleep() {
	time.Sleep(time.Duration(r.Intn(5)) * time.Millisecond)
}

func setup(db *sql.DB) {
	_, err := db.Exec(setupSql)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		_, err := db.Exec(`INSERT INTO products (product_name) VALUES ("computer");`)
		if err != nil {
			log.Fatalf("filling up products table failed. Exec error=%s", err)
		}
	}
}

func read(db *sql.DB, mu *sync.Mutex, i, count int) {
	// mu.Lock()
	// defer mu.Unlock()
	rows, err := db.Query(`SELECT MAX(id) FROM products`)
	if err != nil {
		log.Fatalf("\nproducts select %d/%d. Query error=%s\n", i, count, err)
	} else {
		fmt.Printf(".")
		rows.Close()
	}

}

func write(db *sql.DB, mu *sync.Mutex, i, count int) {
	// mu.Lock()
	// defer mu.Unlock()
	go func() {
		_, err := db.Exec(`INSERT INTO products (product_name) VALUES ("computer");`)
		if err != nil {
			log.Fatalf("user insert. Exec error=%s", err)
		}
	}()
	_, err := db.Exec(`INSERT INTO users (user_name) VALUES ("Bobby");`)
	if err != nil {
		log.Fatalf("user insert. Exec error=%s", err)
	}

	// _, err = result.LastInsertId()
	// if err != nil {
	// 	fmt.Printf("user writer. LastInsertId error=%s", err)
	// }

	fmt.Printf("+")
}
