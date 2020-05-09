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
	"strings"
)

const tableLSNKeyPrefix = "table_lsn_"

type mergeTreeMutationsTable struct {
	genericTable
	distributedServers []*sql.DB
	keyColIndex        int
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
	for i, col := range t.tupleColumns {
		if col.IsKey {
			t.keyColIndex = i
			break
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

// Update handles incoming update DML operation
func (t *mergeTreeMutationsTable) Update(lsn utils.LSN, old, new message.Row) (bool, error) {
	if equal, _ := t.compareRows(old, new); equal {
		return false, nil
	}
	err := t.mergeFunc()
	if err != nil {
		return false, err
	}
	tuples := t.convertTuples(new)
	pkey := tuples[t.keyColIndex]
	tuplesWithoutKey := make([]interface{}, len(tuples)-1)
	copy(tuplesWithoutKey[0:t.keyColIndex], tuples[0:t.keyColIndex])
	copy(tuplesWithoutKey[t.keyColIndex:], tuples[t.keyColIndex+1:])
	colsWithoutKey := make([]message.Column, len(tuples)-1)
	copy(colsWithoutKey[0:t.keyColIndex], t.tupleColumns[0:t.keyColIndex])
	copy(colsWithoutKey[t.keyColIndex:], t.tupleColumns[t.keyColIndex+1:])
	updateClauses := make([]string, len(colsWithoutKey))
	for i, c := range colsWithoutKey {
		updateClauses[i] = c.Name + "=?"
	}

	if t.cfg.IsDistributed {
		query := fmt.Sprintf(`ALTER TABLE %s UPDATE %s WHERE %s=?`, t.cfg.ChPartTable, strings.Join(updateClauses, ","),
			t.tupleColumns[t.keyColIndex].Name)
		for _, conn := range t.distributedServers {
			_, err = conn.Exec(query, append(tuplesWithoutKey, pkey)...)
			if err != nil {
				return false, err
			}
		}
	} else {
		query := fmt.Sprintf(`ALTER TABLE %s UPDATE %s WHERE %s=?`, t.cfg.ChMainTable, strings.Join(updateClauses, ","),
			t.tupleColumns[t.keyColIndex].Name)
		_, err = t.chConn.Exec(query, append(tuplesWithoutKey, pkey)...)
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
		query := fmt.Sprintf(`ALTER TABLE %s DELETE WHERE %s=?`, t.cfg.ChPartTable, t.tupleColumns[t.keyColIndex].Name)
		for _, conn := range t.distributedServers {
			_, err = conn.Exec(query, append(tuples, pkey)...)
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
