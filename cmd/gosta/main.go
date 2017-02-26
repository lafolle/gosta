package main

import (
	"flag"
	"fmt"

	"github.com/lafolle/gosta"
)

var (
	stacksFileName = flag.String("f", "", "file name of stack traces")
	endpoint       = flag.String("u", "", "url from which to fetch stack traces")

	db          = flag.String("db", "sqlite3", "db to which stack traces are dumped")
	dbPassword  = flag.String("p", "passwd", "password connecting to db")
	dbName      = flag.String("n", "gst", "db name")
	dbTablename = flag.String("t", "", "table in which stacks be injected.  If empty,  name will be gosta_<datetime>")
)

func main() {
	flag.Parse()

	if *endpoint != "" && *stacksFileName != "" {
		fmt.Println("Only 1 of -u and -f should be mentioned.")
		return
	}

	if !gosta.IsDbSupported(*db) {
		return
	}

	err := gosta.Process(*stacksFileName, gosta.Options{
		Db:          *db,
		DbPassword:  *dbPassword,
		DbName:      *dbName,
		DbTableName: *dbTablename,
	})
	if err != nil {
		fmt.Println("err processsing stacks:", err)
	}
}
