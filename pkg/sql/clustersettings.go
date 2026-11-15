package sql

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
)

// Cluster settings (SET CLUSTER SETTING name = value, SHOW CLUSTER
// SETTING name): values that hold for the whole cluster, stored at
// keys.ClusterSettingKey and read by every node. Only the settings
// registered here exist; setting one is for admins.

// ClusterSettingKeyRate is the key service's rate limit (issue #310):
// how many uses of the key service functions one role may make per
// second on one node; 0 is no limit.
const ClusterSettingKeyRate = "key_service.rate_limit"

// DefaultKeyServiceRate is ClusterSettingKeyRate's default.
const DefaultKeyServiceRate = 100

type clusterSetting struct {
	def  string
	doc  string
	norm func(v string) (string, error)
}

var clusterSettings = map[string]clusterSetting{
	ClusterSettingKeyRate: {
		def: strconv.Itoa(DefaultKeyServiceRate),
		doc: "uses of datax_key_wrap, datax_key_unwrap, datax_encrypt and datax_decrypt one role may make per second on one node (0: no limit)",
		norm: func(v string) (string, error) {
			n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if err != nil || n < 0 || n > 1_000_000 {
				return "", fmt.Errorf("want a whole number of operations per second from 0 (no limit) to 1000000, got %q", v)
			}
			return strconv.FormatInt(n, 10), nil
		},
	},
}

// ReadClusterSetting returns the value of cluster setting name: what SET
// CLUSTER SETTING stored, or its default.
func ReadClusterSetting(ctx context.Context, db *kvclient.DB, name string) (string, error) {
	cs, ok := clusterSettings[name]
	if !ok {
		return "", newErrf(CodeUndefinedObject, "unknown cluster setting %q", name)
	}
	raw, err := db.Get(ctx, keys.ClusterSettingKey(name))
	if err != nil {
		return "", err
	}
	if raw == nil {
		return cs.def, nil
	}
	return string(raw), nil
}

func unknownClusterSetting(name string) *Error {
	names := make([]string, 0, len(clusterSettings))
	for n := range clusterSettings {
		names = append(names, n)
	}
	sort.Strings(names)
	return newErrf(CodeUndefinedObject, "unknown cluster setting %q (cluster settings: %s)", name, strings.Join(names, ", "))
}

func (s *Session) execSetClusterSetting(ctx context.Context, t *parser.SetClusterSetting) (*Result, *Error) {
	cs, ok := clusterSettings[t.Name]
	if !ok {
		return nil, unknownClusterSetting(t.Name)
	}
	value := cs.def
	if !t.Default {
		v, err := cs.norm(t.Value)
		if err != nil {
			return nil, newErrf(CodeInvalidParameterValue, "cluster setting %s: %v", t.Name, err)
		}
		value = v
	}
	var before string
	err := s.db.RunTxn(ctx, "set-cluster-setting", func(ctx context.Context, txn *kvclient.Txn) error {
		if err := s.checkAdmin(ctx, txn); err != nil {
			return err
		}
		raw, err := txn.Get(ctx, keys.ClusterSettingKey(t.Name))
		if err != nil {
			return err
		}
		before = cs.def
		if raw != nil {
			before = string(raw)
		}
		if t.Default {
			return txn.Delete(ctx, keys.ClusterSettingKey(t.Name))
		}
		return txn.Put(ctx, keys.ClusterSettingKey(t.Name), []byte(value))
	})
	if err != nil {
		return nil, ToSQLError(err)
	}
	log.Audit("cluster-setting", "target", t.Name, "value", value, "was", before, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "SET CLUSTER SETTING"}, nil
}

func (s *Session) execShowClusterSetting(ctx context.Context, t *parser.ShowClusterSetting) (*Result, *Error) {
	if _, ok := clusterSettings[t.Name]; !ok {
		return nil, unknownClusterSetting(t.Name)
	}
	v, err := ReadClusterSetting(ctx, s.db, t.Name)
	if err != nil {
		return nil, ToSQLError(err)
	}
	return &Result{Columns: []ResultColumn{{Name: t.Name, Type: types.String}}, Rows: [][]types.Datum{{types.NewString(v)}}, Tag: "SHOW"}, nil
}
