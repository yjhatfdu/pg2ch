package utils

import (
	"fmt"

	"github.com/mkabilov/pg2ch/pkg/config"
)

var pgToChMap = map[string]string{
	PgSmallint:                 ChInt16,
	PgInteger:                  ChInt32,
	PgBigint:                   ChInt64,
	PgCharacterVarying:         ChString,
	PgVarchar:                  ChString,
	PgText:                     ChString,
	PgReal:                     ChFloat32,
	PgDoublePrecision:          ChFloat64,
	PgInterval:                 ChInt32,
	PgBoolean:                  ChUInt8,
	PgDecimal:                  ChDecimal,
	PgNumeric:                  ChDecimal,
	PgCharacter:                ChFixedString,
	PgChar:                     ChFixedString,
	PgJsonb:                    ChString,
	PgJson:                     ChString,
	PgUuid:                     ChUUID,
	PgBytea:                    ChUInt8Array,
	PgInet:                     ChInt64,
	PgTimestamp:                ChDateTime,
	PgTimestampWithTimeZone:    ChDateTime,
	PgTimestampWithoutTimeZone: ChDateTime,
	PgDate:                     ChDate,
	PgTime:                     ChUint32,
	PgTimeWithoutTimeZone:      ChUint32,
	PgTimeWithTimeZone:         ChUint32,
}

// ToClickHouseType converts pg type into clickhouse type
func ToClickHouseType(pgColumn config.PgColumn) (string, error) {
	chType, ok := pgToChMap[pgColumn.BaseType]
	if !ok {
		return "", fmt.Errorf("could not convert type %s", pgColumn.BaseType)
	}

	switch pgColumn.BaseType {
	case "decimal":
		fallthrough
	case "numeric":
		if pgColumn.Ext == nil {
			return "", fmt.Errorf("precision must be specified for the numeric type")
		}
		chType = fmt.Sprintf("%s(%d, %d)", chType, pgColumn.Ext[0], pgColumn.Ext[1])
	case "character":
		fallthrough
	case "char":
		if pgColumn.Ext == nil {
			return "", fmt.Errorf("length must be specified for character type")
		}
		chType = fmt.Sprintf("%s(%d)", chType, pgColumn.Ext[0])
	}

	if pgColumn.IsArray {
		chType = fmt.Sprintf("Array(%s)", chType)
	}

	if pgColumn.IsNullable && !pgColumn.IsArray {
		chType = fmt.Sprintf("Nullable(%s)", chType)
	}

	return chType, nil
}