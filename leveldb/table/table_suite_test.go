package table

import (
	"testing"

	"github.com/SwanSpouse/goleveldb/leveldb/testutil"
)

func TestTable(t *testing.T) {
	testutil.RunSuite(t, "Table Suite")
}
