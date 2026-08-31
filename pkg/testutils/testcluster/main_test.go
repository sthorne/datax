package testcluster

import (
	"os"
	"testing"

	"github.com/sthorne/datax/pkg/util/log"
)

func TestMain(m *testing.M) {
	if os.Getenv("DATAX_TEST_VERBOSE") != "" {
		log.SetVerbose(true)
	}
	os.Exit(m.Run())
}
