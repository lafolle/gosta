package gosta

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Options struct {
	Db          string
	DbName      string
	DbPassword  string
	DbTableName string
}

type stackTrace struct {
	// goroutine id.
	id int

	// if goroutine is running.
	running bool

	// if goroutine is locked to thread.
	lockedToThread bool

	// receiver address
	receiverAddr string

	// if goroutine is runnable.
	runnable bool

	// Can be either of
	// 1. syscall
	// 2. semacquire,
	// 3. IO wait
	// 4. chan receive
	// 5. chan send
	// 6. select
	waitReason string

	// how long it go routine has been stalled.
	stalledSince time.Duration

	body string
}

var (
	supportedDbs = map[string]struct{}{
		"sqlite3": struct{}{},
	}
	all []stackTrace

	createTableStmt string = "create table %s (id int primary key not null, running boolean, runnable boolean, lockedToThread boolean, waitReason varchar(64), stalledSince interval, body text)"

	insertStackStmt string = "insert into %s (id, running, runnable, lockedToThread, waitReason, stalledSince, body) values ($1, $2, $3, $4, $5, $6, $7)"
)

func IsDbSupported(dbname string) bool {
	_, ok := supportedDbs[dbname]
	return ok
}

func Process(fname string, opts Options) error {

	fd, err := os.Open(fname)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer fd.Close()

	all = make([]stackTrace, 0)

	defer fd.Close()
	var st stackTrace

	scanner := bufio.NewScanner(fd)

	for scanner.Scan() {
		text := scanner.Text()
		if text == "\n" || len(text) == 0 {
			all = append(all, st)
			continue
		}
		if isHeader(text) {
			running, runnable, lockedToThread, waitReason, id, stalledSince := parseSHeader(text)
			st = stackTrace{
				id:             id,
				running:        running,
				lockedToThread: lockedToThread,
				runnable:       runnable,
				waitReason:     waitReason,
				stalledSince:   stalledSince,
				body:           text,
			}
		} else {
			st.body += "\n" + text
		}
	}

	db, err := sql.Open(opts.Db, opts.DbName)
	defer func() {
		if err := db.Close(); err != nil {
			fmt.Println("close db:", err)
		}
	}()
	if err != nil {
		return err
	}

	if err := db.Ping(); err != nil {
		return err
	}

	if opts.DbTableName == "" {
		opts.DbTableName = fmt.Sprintf("gosta_%s", time.Now().Format("20060102T150405"))
	}

	// create new table.
	fmt.Printf("creating table %s in sqlite3...\n", opts.DbTableName)
	createTableStmt, err := db.Prepare(fmt.Sprintf(createTableStmt, opts.DbTableName))
	if err != nil {
		fmt.Println("createTableStmt:", err)
		return err
	}
	_, err = createTableStmt.Exec()
	if err != nil {
		return err
	}

	insertStmt, err := db.Prepare(fmt.Sprintf(insertStackStmt, opts.DbTableName))
	if err != nil {
		return err
	}
	defer insertStmt.Close()
	fmt.Printf("writing %d stack traces to table %s...", len(all), opts.DbTableName)
	for _, s := range all {
		_, err = insertStmt.Exec(s.id, s.running, s.runnable, s.lockedToThread, s.waitReason, fmt.Sprintf(`'%.0f minutes'`, s.stalledSince.Minutes()), s.body)
		if err != nil {
			fmt.Printf("err inserting:%s stack:%+v", err, s)
			return err
		}
	}
	fmt.Println("done")
	return nil
}

// line ending with colon is header.
func isHeader(line string) bool { return line[len(line)-1] == ':' }

func parseSHeader(header string) (running, runnable, lockedToThread bool, waitReason string, id int, stalledSince time.Duration) {
	id, _ = strconv.Atoi(strings.Split(header, " ")[1])
	op := strings.Index(header, "[")
	cl := strings.Index(header, "]")
	if op == -1 || cl == -1 {
		panic("invalid header:" + header)
	}
	x := strings.Split(header[op+1:cl], ",")
	switch len(x) {
	case 1:
		if x[0] == "running" {
			running = true
			runnable = true
		} else if x[0] == "runnable" {
			running = false
			runnable = true
		} else {
			waitReason = x[0]
		}
	case 2:
		waitReason = x[0]
		if strings.Contains(x[1], "minutes") {
			stalledSince = getDuration(x[1])
		} else if strings.Contains(x[1], "locked to thread") {
			lockedToThread = true
		}
	case 3:
		waitReason = x[0]
		if strings.Contains(x[1], "minutes") {
			stalledSince = getDuration(x[1])
		}
		lockedToThread = true
	default:
		errors.New("invalid header")
	}
	return
}

func getDuration(text string) time.Duration {
	i := strings.Index(text, "minutes")
	j, err := strconv.Atoi(strings.Trim(text[0:i], " "))
	if err != nil {
		panic(fmt.Sprintf("failed to convert string to number: err:%s, text:%s", err, text[0:i]))
	}
	return time.Duration(j) * time.Minute
}

/*
create table g (
	id int primary key not null,
	running boolean,
	runnable boolean,
	waitReason varchar(32),
	stalledSince interval,
	body text
)
*/
//dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", *dbUser, *dbPassword, *dbName)
