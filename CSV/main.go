package main

import (
	"os"
	"log"
	"bufio"
	"strings"
	"database/sql"
	_ "github.com/denisenkom/go-mssqldb"
	"fmt"
	"strconv"
	"time"
)

func main()  {

	db, err := sql.Open("sqlserver", "sqlserver://test:test@localhost?database=pl-retrotest&connection+timeout=30")
	if err != nil {
		log.Fatal("[DB OPEN]",err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		log.Fatal("[DB PING]", err)
	}

	fileName := "_04.tsv"
	//04_06.tsv
	//06_08.tsv
	//08_10.tsv
	//10_.tsv

	file,err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	f := bufio.NewReader(file)
	count := 0
	for {
		line, err := f.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}
		count++
		line = strings.TrimRight(line,"\n")
		col := strings.Split(line,"\t")
		if len(col) == 11 {
			query := "INSERT INTO [cl_1_raw] (INN, UserID, txDate, KKM, City, GoodsCat, Goods, Ed, Kol, Summ, TypeOfPay, Year, Month, Day, DayW, YearMonth) VALUES (@p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12,@p13,@p14,@p15,@p16)"
			stmt, _ := db.Prepare(query)

			txDate := col[2]
			t, _ := time.Parse("2006-01-02 15:04:05", col[2])

			kol,_ := strconv.ParseFloat(col[8],9)
			price,_ := strconv.ParseFloat(col[9],9)
			if price > 500000 || price < -500000 {
				continue
				}
				_, err := stmt.Exec(
					strings.Trim(col[0],"'"),
					strings.Trim(col[1],"'"),
					txDate,
					col[3],
					col[4],
					col[5],
					col[6],
					col[7],
					kol,
					price,
					col[10],
					t.Year(),
					t.Month(),
					t.Day(),
					t.Weekday(),
					fmt.Sprintf("%d-%d", t.Year(), t.Month()),
				)

			if err != nil {
				log.Fatal("[DB Exec] ", err)
			}
			if (count % 10000) == 0 {
				fmt.Printf("Row: %d\n", count)
			}
		} else {
			log.Printf("[ERR] %d) %v\n", count, line)
		}

	}
}
