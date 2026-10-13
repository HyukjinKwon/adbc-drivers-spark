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
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// showClient answers every query with the same names and records the
// queries. Each row repeats the name in both columns, so the result works
// for SHOW CATALOGS and SHOW SCHEMAS (name first) and SHOW TABLES (name
// second). Only ExecuteQuery is implemented.
type showClient struct {
	SparkClient
	names   []string
	queries []string
}

func (c *showClient) ExecuteQuery(_ context.Context, q QueryContext) (array.RecordReader, int64, error) {
	c.queries = append(c.queries, q.Query)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "namespace", Type: arrow.BinaryTypes.String},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.StringBuilder).AppendValues(c.names, nil)
	b.Field(1).(*array.StringBuilder).AppendValues(c.names, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	return rdr, -1, err
}

func TestNamePatternsMatchOnTheClient(t *testing.T) {
	names := []string{"my_table", "myXtable", "MY2", "a.b", "axb", "other"}
	for _, tc := range []struct {
		pattern *string
		want    []string
	}{
		{nil, names},
		{new(""), names},
		{new("%"), names},
		{new("my%"), []string{"my_table", "myXtable", "MY2"}},
		// '_' matches any one character.
		{new("my_table"), []string{"my_table", "myXtable"}},
		// '.' is literal, unlike in Spark's own patterns.
		{new("a.b"), []string{"a.b"}},
		// Matching is case-insensitive, as in Spark.
		{new("my2"), []string{"MY2"}},
		// '*' is literal too.
		{new("my*"), nil},
	} {
		name := "nil"
		if tc.pattern != nil {
			name = strconv.Quote(*tc.pattern)
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			c := &showClient{names: names}

			catalogs, err := DefaultGetCatalogsImpl(c, ctx, tc.pattern)
			require.NoError(t, err)
			require.Equal(t, tc.want, catalogs)

			schemas, err := DefaultGetDBSchemasForCatalogImpl(c, ctx, "cat", tc.pattern)
			require.NoError(t, err)
			require.Equal(t, tc.want, schemas)

			tables, err := DefaultGetTablesForDBSchemaImpl(c, ctx, "cat", "db", tc.pattern, nil, false)
			require.NoError(t, err)
			var tableNames []string
			for _, table := range tables {
				tableNames = append(tableNames, table.TableName)
			}
			require.Equal(t, tc.want, tableNames)

			// Spark gets no LIKE clause; it would read the pattern differently.
			require.Equal(t, []string{
				"SHOW CATALOGS",
				"SHOW SCHEMAS IN `cat`",
				"SHOW TABLES IN `cat`.`db`",
			}, c.queries)
		})
	}
}
