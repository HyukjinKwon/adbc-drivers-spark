// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sparkbase

import (
	"context"
	"strings"
	"testing"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// describeClient lists tables and describes each with one INT column id,
// except that DESCRIBE TABLE fails for the tables in broken, and for every
// table once the context is done. Only ExecuteQuery is implemented.
type describeClient struct {
	SparkClient
	tables []string
	broken map[string]error
}

func (c *describeClient) ExecuteQuery(ctx context.Context, q QueryContext) (array.RecordReader, int64, error) {
	if strings.HasPrefix(q.Query, "SHOW TABLES") {
		return stringRows([]string{"namespace", "tableName"}, func(i int) []string {
			return []string{"db", c.tables[i]}
		}, len(c.tables))
	}
	if err := ctx.Err(); err != nil {
		return nil, -1, err
	}
	for name, err := range c.broken {
		if strings.HasSuffix(q.Query, "."+QuoteIdentifier(name)) {
			return nil, -1, err
		}
	}
	return stringRows([]string{"col_name", "data_type", "comment"}, func(int) []string {
		return []string{"id", "int", ""}
	}, 1)
}

// stringRows returns n rows of string columns, built by row.
func stringRows(names []string, row func(i int) []string, n int) (array.RecordReader, int64, error) {
	fields := make([]arrow.Field, len(names))
	for i, name := range names {
		fields[i] = arrow.Field{Name: name, Type: arrow.BinaryTypes.String}
	}
	schema := arrow.NewSchema(fields, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for i := range n {
		for j, value := range row(i) {
			b.Field(j).(*array.StringBuilder).Append(value)
		}
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	return rdr, -1, err
}

func TestTablesThatFailToDescribeAreListedWithoutColumns(t *testing.T) {
	c := &describeClient{
		tables: []string{"a", "broken", "b"},
		broken: map[string]error{"broken": adbc.Error{
			Code: adbc.StatusNotFound,
			Msg:  "[spark] [TABLE_OR_VIEW_NOT_FOUND] The table or view `broken` cannot be found.",
		}},
	}

	tables, err := DefaultGetTablesForDBSchemaImpl(c, context.Background(), "cat", "db", nil, nil, true)
	require.NoError(t, err)
	require.Len(t, tables, 3)
	for _, table := range tables {
		if table.TableName == "broken" {
			require.Equal(t, []driverbase.ColumnInfo{}, table.TableColumns)
			continue
		}
		require.Len(t, table.TableColumns, 1, table.TableName)
		require.Equal(t, "id", table.TableColumns[0].ColumnName)
	}
}

func TestDescribeFailsTheListingWhenCancelled(t *testing.T) {
	c := &describeClient{tables: []string{"a", "b"}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := DefaultGetTablesForDBSchemaImpl(c, ctx, "cat", "db", nil, nil, true)
	require.ErrorIs(t, err, context.Canceled)
}
