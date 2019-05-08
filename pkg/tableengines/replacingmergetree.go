package tableengines

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils"
)

type replacingMergeTree struct {
	genericTable

	verColumn string
}

// NewReplacingMergeTree instantiates replacingMergeTree
func NewReplacingMergeTree(ctx context.Context, conn *sql.DB, tblCfg config.Table) *replacingMergeTree {
	t := replacingMergeTree{
		genericTable: newGenericTable(ctx, conn, tblCfg),
		verColumn:    tblCfg.VerColumn,
	}
	t.chUsedColumns = append(t.chUsedColumns, tblCfg.VerColumn, tblCfg.IsDeletedColumn)

	t.flushQueries = []string{fmt.Sprintf("INSERT INTO %[1]s (%[2]s) SELECT %[2]s FROM %[3]s ORDER BY %[4]s",
		t.cfg.ChMainTable, strings.Join(t.chUsedColumns, ", "), t.cfg.ChBufferTable, t.cfg.BufferTableRowIdColumn)}

	return &t
}

// Sync performs initial sync of the data; pgTx is a transaction in which temporary replication slot is created
func (t *replacingMergeTree) Sync(pgTx *pgx.Tx) error {
	return t.genSync(pgTx, t)
}

// Write implements io.Writer which is used during the Sync process, see genSync method
func (t *replacingMergeTree) Write(p []byte) (int, error) {
	var row []interface{}

	row, n, err := t.syncConvertIntoRow(p)
	if err != nil {
		return 0, err
	}
	row = append(row, 0, 0) // append "version" and "is_deleted" columns

	return n, t.insertRow(row)
}

// Insert handles incoming insert DML operation
func (t *replacingMergeTree) Insert(lsn utils.LSN, new message.Row) (bool, error) {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(new), uint64(lsn), 0),
	})
}

// Update handles incoming update DML operation
func (t *replacingMergeTree) Update(lsn utils.LSN, old, new message.Row) (bool, error) {
	equal, keyChanged := t.compareRows(old, new)
	if equal {
		return t.processCommandSet(nil)
	}

	if keyChanged {
		return t.processCommandSet(commandSet{
			append(t.convertTuples(old), uint64(lsn), 1),
			append(t.convertTuples(new), uint64(lsn), 0),
		})
	}

	return t.processCommandSet(commandSet{
		append(t.convertTuples(new), uint64(lsn), 0),
	})
}

// Delete handles incoming delete DML operation
func (t *replacingMergeTree) Delete(lsn utils.LSN, old message.Row) (bool, error) {
	return t.processCommandSet(commandSet{
		append(t.convertTuples(old), uint64(lsn), 1),
	})
}
