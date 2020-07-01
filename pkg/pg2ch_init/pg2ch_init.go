package pg2ch_init

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

type config struct {
	PgDSN                    string   `json:"pgDSN"`
	ClickhouseMasterDSN      string   `json:"clickhouseMasterDSN"`
	ClickhouseSegmentsDSN    []string `json:"clickhouseSegmentsDSN"`
	ClickhouseClusterName    string   `json:"clickhouseClusterName"`
	ClickhouseRemoteDatabase string   `json:"clickhouseRemoteDatabase"`
	Tables                   []table  `json:"tables"`
}

type table struct {
	Schema     string `json:"schema"`
	Name       string `json:"name"`
	PrimaryKey string `json:"primaryKey"`
	Distribute string `json:"distribute"`
	Partition  string `json:"partition"`
}

var quite = false

var typeConvert = map[string]string{
	"text":      "String",
	"varchar":   "String",
	"int2":      "Int16",
	"int4":      "Int32",
	"int8":      "Int64",
	"bool":      "UInt8",
	"timestamp": "DateTime",
	"interval":  "Int64",
	"numeric":   "Decimal(12,4)",
}

var columnBlackList = map[string]bool{
	"etl_time": true,
}

func bindConfig(path string) config {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	data, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatal(err)
	}
	var c config
	err = json.Unmarshal(data, &c)
	if err != nil {
		log.Fatal(err)
	}
	return c
}
func execQueries(conn *sql.DB, stmts []string, dryRun bool) {
	for _, s := range stmts {
		if !quite {
			fmt.Println(s)
		}
		if !dryRun {
			_, err := conn.Exec(s)
			if err != nil {
				log.Fatalf("ch exec error: %v", err)
			}
		}
	}
}

func (t table) create(c config, pgConn *sql.DB, ckMaster *sql.DB, ckSegs []*sql.DB, dryRun bool, hasSegments bool) {
	columns := t.fetchColumnInfo(pgConn)
	bufTable := t.generateChTable(c, columns, "buf")
	execQueries(ckMaster, bufTable, dryRun)
	if hasSegments && t.Distribute != "" {
		mainTable := t.generateChTable(c, columns, "main")
		execQueries(ckMaster, mainTable, dryRun)
		partTable := t.generateChTable(c, columns, "part")
		for _, seg := range ckSegs {
			execQueries(seg, partTable, dryRun)
		}
	} else {
		singleTable := t.generateChTable(c, columns, "part")
		execQueries(ckMaster, singleTable, dryRun)
	}
}

type column struct {
	name          string
	columnDefault *string
	nullable      string
	sqlType       string
}

func (c column) gen() string {
	var chType = c.sqlType
	if _, ok := typeConvert[chType]; ok {
		chType = typeConvert[chType]
	}
	//chType = strings.Replace(chType, "numeric", "Decimal", -1)
	if c.nullable == "YES" {
		chType = "Nullable(" + chType + ")"
	}
	var colDefault string
	if c.columnDefault != nil {
		colDefault = *c.columnDefault
	}
	if strings.HasPrefix(colDefault, "nextval") {
		colDefault = ""
	}
	return fmt.Sprintf(`%s %s %s`, c.name, chType, colDefault)
}

func (t table) fetchColumnInfo(pgConn *sql.DB) []column {
	rows, err := pgConn.Query(`select column_name,column_default,is_nullable,udt_name from information_schema.columns 
where table_schema=$1 and table_name=$2 order by ordinal_position;`, t.Schema, t.Name)
	if err != nil {
		log.Fatalf("pg query error: %v", err)
	}
	columns := make([]column, 0)
	for rows.Next() {
		var c column
		err = rows.Scan(&c.name, &c.columnDefault, &c.nullable, &c.sqlType)
		if err != nil {
			log.Fatalf("pg scan error: %v", err)
		}
		if !columnBlackList[c.name] {
			columns = append(columns, c)
		}
	}
	if len(columns) == 0 {
		log.Fatalf("table %s has no columns or not exist", t.Name)
	}
	return columns
}

func (t table) generateChTable(c config, columns []column, tableType string) []string {
	colStr := make([]string, len(columns))
	for i, c := range columns {
		colStr[i] = c.gen()
	}
	cols := strings.Join(colStr, ",\n")
	switch tableType {
	case "main":
		return []string{fmt.Sprintf(`TRUNCATE TABLE IF EXISTS %s;`, t.Name), fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, t.Name),
			fmt.Sprintf(`
CREATE TABLE %s_all (
%s)engine=Distributed(%s,%s,%s,%s);`, t.Name, cols,
				c.ClickhouseClusterName, c.ClickhouseRemoteDatabase, t.Name, t.Distribute)}
	case "buf":
		return []string{fmt.Sprintf(`TRUNCATE TABLE IF EXISTS %s_buf;`, t.Name), fmt.Sprintf(`DROP TABLE IF EXISTS %s_buf;`, t.Name), fmt.Sprintf(`
CREATE TABLE %s_buf (
%s,
row_id UInt64)engine=Memory();`, t.Name, cols)}
	case "part":
		var partition string
		if t.Partition != "" {
			partition = "\nPARTITION BY " + t.Partition
		}
		return []string{fmt.Sprintf(`TRUNCATE TABLE IF EXISTS %s_part;`, t.Name), fmt.Sprintf(`DROP TABLE IF EXISTS %s_part;`, t.Name), fmt.Sprintf(`
CREATE TABLE %s (
%s)engine=MergeTree() %s
ORDER BY %s SETTINGS index_granularity = 8192;`,
			t.Name, cols, partition, t.PrimaryKey)}
	}
	panic("")
}

func Pg2chInit(configFile string, dryRun bool, q bool) {
	quite = q
	c := bindConfig(configFile)
	pgConn, err := sql.Open("postgres", c.PgDSN)
	if err != nil {
		log.Fatalf("pg connection error: %v", err)
	}
	hasSegments := len(c.ClickhouseSegmentsDSN) > 0
	chSegmentsConn := make([]*sql.DB, 0)
	for _, dsn := range c.ClickhouseSegmentsDSN {
		conn, err := sql.Open("clickhouse", dsn)
		if err != nil {
			log.Fatalf("ch connection error: %v", err)
		}
		chSegmentsConn = append(chSegmentsConn, conn)
	}
	chMasterConn, err := sql.Open("clickhouse", c.ClickhouseMasterDSN)
	for _, t := range c.Tables {
		t.create(c, pgConn, chMasterConn, chSegmentsConn, dryRun, hasSegments)
	}
}
