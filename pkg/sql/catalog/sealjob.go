package catalog

import (
	"encoding/json"
	"fmt"
)

// Seal job kinds.
const (
	// SealJobSeal seals an existing table's plain rows (ALTER TABLE ...
	// SET (encryption_key | encryption_key_by)).
	SealJobSeal = "seal"
	// SealJobRotate moves the rows under a rotated key's older versions
	// to its current one (ALTER TABLE ... ROTATE ENCRYPTION KEY,
	// datax_rotate_tenant_key).
	SealJobRotate = "rotate"
)

// SealJob is the record of a table's seal job (keys.SealJobKey; issue
// #310, step 5): a resumable background rewrite of a sealed table's
// primary rows, in batches, each an ordinary transaction that also
// writes this record's progress, so that a job cut short by a restart or
// a move of range 1's leadership goes on from its last batch. The job
// works in passes over its span; it is done when a whole pass, started
// once every gateway seals as the job wants, finds nothing to rewrite.
// Then a seal job clears the descriptor's Sealing, a rotation destroys
// the key's older versions (unless KeepOld), and the record goes.
type SealJob struct {
	TableID uint64 `json:"table_id"`
	// Table is the table's name when the job was made (for display; the
	// job finds the table by id).
	Table string `json:"table"`
	Kind  string `json:"kind"`
	// KeyID and Version, for a rotation: rows sealed under KeyID at a
	// version below Version are rewritten under its current version,
	// and once none is left the versions below Version are destroyed.
	KeyID   int64 `json:"key_id,omitempty"`
	Version int   `json:"version,omitempty"`
	// Tenant, for the rotation of one tenant's key: the key encoding of
	// its tenant column value; the job's span is that tenant's rows.
	Tenant []byte `json:"tenant,omitempty"`
	// KeepOld: a rotation leaves the older versions in place when done.
	KeepOld bool `json:"keep_old,omitempty"`
	// DescVersion, for a seal job: the descriptor version that sealed
	// the table. No row is rewritten before every gateway's lease has
	// adopted it (until then a gateway may write a plain row behind the
	// job). NotBefore (Unix nanoseconds), for a rotation: when every
	// gateway's key cache has seen the new version.
	DescVersion uint64 `json:"desc_version,omitempty"`
	NotBefore   int64  `json:"not_before,omitempty"`
	// Started: the job has seen its condition to start met.
	Started bool `json:"started,omitempty"`
	// Done, for a rotation: no row is left to rewrite; what is left is
	// destroying the older versions (unless KeepOld) and the record.
	Done bool `json:"done,omitempty"`
	// Resume is where the current pass goes on from (nil: the start of
	// the span).
	Resume []byte `json:"resume,omitempty"`
	// Pass counts passes from 1; PassRewritten and PassUnreadable count
	// the current pass's rows rewritten and rows whose key is gone (a
	// destroyed tenant key, an older version destroyed early), which no
	// one can read and the job steps over.
	Pass           int   `json:"pass"`
	PassRewritten  int64 `json:"pass_rewritten,omitempty"`
	PassUnreadable int64 `json:"pass_unreadable,omitempty"`
	// Rewritten counts every row the job has rewritten.
	Rewritten int64 `json:"rewritten"`
	// Created and Updated are Unix nanoseconds.
	Created int64 `json:"created"`
	Updated int64 `json:"updated,omitempty"`
	// LastError is the last batch's failure, cleared by a batch that
	// succeeds; the job retries on its next turn.
	LastError string `json:"last_error,omitempty"`
}

// Encode renders the record.
func (j *SealJob) Encode() ([]byte, error) { return json.Marshal(j) }

// DecodeSealJob reads one record of keys.SealJobSpan.
func DecodeSealJob(raw []byte) (*SealJob, error) {
	var j SealJob
	if err := json.Unmarshal(raw, &j); err != nil {
		return nil, fmt.Errorf("corrupt seal job record: %w", err)
	}
	return &j, nil
}

// State is how the job stands, for display: waiting (for gateways to
// adopt the change it depends on), running, or retrying (its last batch
// failed).
func (j *SealJob) State() string {
	switch {
	case j.LastError != "":
		return "retrying"
	case !j.Started:
		return "waiting"
	}
	return "running"
}
