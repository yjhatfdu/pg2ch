package tableengines

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils"
	"log"
	"strconv"
	"strings"
	"time"
)

const tableLSNKeyPrefix = "table_lsn_"

type mergeTreeMutationsTable struct {
	genericTable
	distributedServers []*sql.DB
	keyColIndex        int
	partitionColIndex  int
	mergeFunc          func() error
	persistStoreFunc   func(key string, val []byte) error
}

func NewMergeTreeMutations(ctx context.Context, conn *sql.DB, tblCfg config.Table, genID *uint64, distributedServers []*sql.DB,
	mergeFunc func() error, persistStoreFunc func(key string, val []byte) error) *mergeTreeMutationsTable {
	if tblCfg.IsDistributed && len(distributedServers) == 0 {
		log.Fatalf("table %v is distributed, but no distributed servers specified.", tblCfg.ChMainTable)
	}
	t := mergeTreeMutationsTable{
		genericTable:       newGenericTable(ctx, conn, tblCfg, genID),
		distributedServers: distributedServers,
		mergeFunc:          mergeFunc,
		persistStoreFunc:   persistStoreFunc,
	}
	t.partitionColIndex = -1
	for i, col := range t.tupleColumns {
		if col.IsKey {
			t.keyColIndex = i
		}
		if col.Name == t.cfg.PartitionKey {
			t.partitionColIndex = i
		}
	}
	if t.cfg.ChBufferTable == "" {
		return &t
	}

	t.flushQueries = []string{fmt.Sprintf("INSERT INTO %[1]s (%[2]s) SELECT %[2]s FROM %[3]s ORDER BY %[4]s",
		t.cfg.ChMainTable, strings.Join(t.chUsedColumns, ", "), t.cfg.ChBufferTable, t.cfg.BufferTableRowIdColumn)}

	return &t
}

// Sync performs initial sync of the data; pgTx is a transaction in which temporary replication slot is created
func (t *mergeTreeMutationsTable) Sync(pgTx *pgx.Tx) error {
	return t.genSync(pgTx, t)
}

// Write implements io.Writer which is used during the Sync process, see genSync method
func (t *mergeTreeMutationsTable) Write(p []byte) (int, error) {
	var row []interface{}

	row, n, err := t.syncConvertIntoRow(p)
	if err != nil {
		return 0, err
	}

	if t.cfg.GenerationColumn != "" {
		row = append(row, 0)
	}

	return n, t.insertRow(row)
}

// Insert handles incoming insert DML operation
func (t *mergeTreeMutationsTable) Insert(lsn utils.LSN, new message.Row) (bool, error) {
	return t.processCommandSet(commandSet{t.convertTuples(new)})
}
func (t *genericTable) convertTuplesForUpdate(row message.Row) []interface{} {
	var err error
	res := make([]interface{}, 0)

	for colId, col := range t.tupleColumns {
		var val interface{}
		if _, ok := t.columnMapping[col.Name]; !ok {
			continue
		}

		if row[colId].Kind != message.TupleNull {
			val, err = convertForUpdate(string(row[colId].Value), t.columnMapping[col.Name], t.cfg.PgColumns[col.Name])
			if err != nil {
				panic(err)
			}
		}

		res = append(res, val)
	}
	if t.cfg.GenerationColumn != "" {
		res = append(res, uint32(*t.generationID))
	}

	return res
}
func convertForUpdate(val string, chType config.ChColumn, pgType config.PgColumn) (interface{}, error) {
	switch chType.BaseType {
	case utils.ChInt8:
		return strconv.ParseInt(val, 10, 8)
	case utils.ChInt16:
		return strconv.ParseInt(val, 10, 16)
	case utils.ChInt32:
		return strconv.ParseInt(val, 10, 32)
	case utils.ChInt64:
		return strconv.ParseInt(val, 10, 64)
	case utils.ChUInt8:
		if pgType.BaseType == utils.PgBoolean {
			if val == pgTrue {
				return 1, nil
			} else if val == pgFalse {
				return 0, nil
			}
		}

		return nil, fmt.Errorf("can't convert %v to %v", pgType.BaseType, chType.BaseType)
	case utils.ChUInt16:
		return strconv.ParseUint(val, 10, 16)
	case utils.ChUint32:
		if pgType.BaseType == utils.PgTimeWithoutTimeZone {
			t, err := time.Parse("15:04:05", val)
			if err != nil {
				return nil, err
			}

			return t.Hour()*3600 + t.Minute()*60 + t.Second(), nil
		}

		return strconv.ParseUint(val, 10, 32)
	case utils.ChUint64:
		return strconv.ParseUint(val, 10, 64)
	case utils.ChFloat32:
		return strconv.ParseFloat(val, 32)
	case utils.ChDecimal:
		//segs := strings.Split(val, ".")
		//		//if len(segs) == 1 {
		//		//	vi, err := strconv.ParseInt(segs[0], 10, 64)
		//		//	if err != nil {
		//		//		return nil, err
		//		//	}
		//		//	return vi * 10000, nil
		//		//} else if len(segs) == 2 {
		//		//	lfrac := len(segs[1])
		//		//	v := strings.Replace(val, ".", "", 1)
		//		//	vi, err := strconv.ParseInt(v, 10, 64)
		//		//	if err != nil {
		//		//		return nil, err
		//		//	}
		//		//
		//		//	return vi * factors10[4-lfrac], nil
		//		//} else {
		//		//	return nil, errors.New("invalid numeric: " + val)
		//		//}
		return fmt.Sprintf(`toDecimal64('%s',4)`, val), nil
	case utils.ChFloat64:
		return strconv.ParseFloat(val, 64)
	case utils.ChFixedString:
		fallthrough
	case utils.ChString:
		return val, nil
	case utils.ChDate:
		return time.Parse("2006-01-02", val[:10])
	case utils.ChDateTime:
		if len(val) == 29 {
			return time.Parse("2006-01-02 15:04:05.999999999Z07", val)
		}
		return time.ParseInLocation("2006-01-02 15:04:05", val[:19], time.Local)
	case utils.ChUUID:
		return val, nil
	}

	return nil, fmt.Errorf("unknown type: %v", chType)
}

// Update handles incoming update DML operation
func (t *mergeTreeMutationsTable) Update(lsn utils.LSN, old, new message.Row) (bool, error) {
	if equal, _ := t.compareRows(old, new); equal {
		return false, nil
	}
	err := t.mergeFunc()
	if err != nil {
		return false, err
	}
	tuples := t.convertTuplesForUpdate(new)
	pkey := tuples[t.keyColIndex]
	tuplesWithoutKey := make([]interface{}, len(tuples)-1)
	copy(tuplesWithoutKey[0:t.keyColIndex], tuples[0:t.keyColIndex])
	copy(tuplesWithoutKey[t.keyColIndex:], tuples[t.keyColIndex+1:])
	colsWithoutKey := make([]message.Column, len(tuples)-1)
	copy(colsWithoutKey[0:t.keyColIndex], t.tupleColumns[0:t.keyColIndex])
	copy(colsWithoutKey[t.keyColIndex:], t.tupleColumns[t.keyColIndex+1:])
	updateClauses := make([]string, 0)
	tuplesWithoutNull := make([]interface{}, 0)
	for i, c := range colsWithoutKey {
		var partitionColIndex = t.partitionColIndex
		if partitionColIndex > t.keyColIndex {
			partitionColIndex--
		}
		if i == partitionColIndex {
			continue
		}
		if tuplesWithoutKey[i] == nil {
			updateClauses = append(updateClauses, c.Name+"=NULL")
		} else if t.columnMapping[ c.Name].BaseType == utils.ChDecimal {
			updateClauses = append(updateClauses, c.Name+"="+tuplesWithoutKey[i].(string))
		} else {
			updateClauses = append(updateClauses, c.Name+"=?")
			tuplesWithoutNull = append(tuplesWithoutNull, tuplesWithoutKey[i])
		}
	}
	if t.cfg.IsDistributed {
		//wait for distributed table send 100ms
		time.Sleep(120 * time.Millisecond)
		query := fmt.Sprintf(`ALTER TABLE %s UPDATE %s WHERE %s=?`, t.cfg.ChPartTable, strings.Join(updateClauses, ","),
			t.tupleColumns[t.keyColIndex].Name)
		for _, conn := range t.distributedServers {
			_, err = conn.Exec(query, append(tuplesWithoutNull, pkey)...)
			if err != nil {
				return false, err
			}
		}
	} else {
		query := fmt.Sprintf(`ALTER TABLE %s UPDATE %s WHERE %s=?`, t.cfg.ChMainTable, strings.Join(updateClauses, ","),
			t.tupleColumns[t.keyColIndex].Name)
		_, err = t.chConn.Exec(query, append(tuplesWithoutNull, pkey)...)
		if err != nil {
			return false, err
		}
	}
	if err := t.persistStoreFunc(tableLSNKeyPrefix+t.cfg.PgTableName.String(), lsn.Bytes()); err != nil {
		return false, fmt.Errorf("could not store lsn for table %s", t.cfg.PgTableName.String())
	}
	log.Println("update one row")
	return false, nil
}

// Delete handles incoming delete DML operation
func (t *mergeTreeMutationsTable) Delete(lsn utils.LSN, old message.Row) (bool, error) {
	err := t.mergeFunc()
	if err != nil {
		return false, err
	}
	tuples := t.convertTuples(old)
	pkey := tuples[t.keyColIndex]
	if t.cfg.IsDistributed {
		//wait for distributed table send 100ms
		time.Sleep(120 * time.Millisecond)
		query := fmt.Sprintf(`ALTER TABLE %s DELETE WHERE %s=?`, t.cfg.ChPartTable, t.tupleColumns[t.keyColIndex].Name)
		for _, conn := range t.distributedServers {
			_, err = conn.Exec(query, pkey)
			if err != nil {
				return false, err
			}
		}
	} else {
		query := fmt.Sprintf(`ALTER TABLE %s DELETE WHERE %s=?`, t.cfg.ChMainTable, t.tupleColumns[t.keyColIndex].Name)
		_, err = t.chConn.Exec(query, pkey)
		if err != nil {
			return false, err
		}
	}
	if err := t.persistStoreFunc(tableLSNKeyPrefix+t.cfg.PgTableName.String(), lsn.Bytes()); err != nil {
		return false, fmt.Errorf("could not store lsn for table %s", t.cfg.PgTableName.String())
	}
	log.Println("delete one row")
	return false, nil
}
