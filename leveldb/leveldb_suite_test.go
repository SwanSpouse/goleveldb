package leveldb

import (
	"testing"

	"github.com/SwanSpouse/goleveldb/leveldb/testutil"
)

func TestLevelDB(t *testing.T) {
	testutil.RunSuite(t, "LevelDB Suite")
}
