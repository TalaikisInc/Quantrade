package database

import (
	"database/sql"
	"os"

	_ "github.com/lib/pq"
)

func Connect() *sql.DB {
	db, err := sql.Open("postgres", "user="+os.Getenv("DATABASE_USER")+
		" password="+os.Getenv("DATABASE_PASSWORD")+
		" dbname="+os.Getenv("DATABASE_NAME")+" sslmode=disable")
	if err != nil {
		panic(err)
	}

	return db
}
