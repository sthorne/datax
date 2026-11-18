#!/usr/bin/env bash
# ci-local.sh runs, on this machine, what .github/workflows/ci.yaml and
# vulncheck.yaml run on GitHub: the gate a change passes before it is
# merged while the hosted workflows are paused (docs/testing.md).
#
#   scripts/ci-local.sh            lint, unit tests and the four cluster shards
#   scripts/ci-local.sh lint       gofmt, vet and staticcheck only
#   scripts/ci-local.sh unit       go test -race on every package but testcluster
#   scripts/ci-local.sh cluster    go test -race on testcluster, all four shards
#   scripts/ci-local.sh vulncheck  govulncheck (needs vuln.go.dev)
#
# Each step's output is written to $CI_LOCAL_OUT (default ./ci-local) and
# the script exits non-zero if any step failed; the failures are repeated
# at the end, as the hosted jobs do. The cluster shards run one after the
# other, each with the hosted job's 30-minute limit. Chrome and pg_dump
# must be present: the browser and pg_dump tests are required to run,
# not skip (DATAX_REQUIRE_BROWSER, DATAX_REQUIRE_PGDUMP). Set
# DATAX_CHROME and DATAX_PG_DUMP when they are not on the default path.
set -uo pipefail

cd "$(dirname "$0")/.."
out=${CI_LOCAL_OUT:-ci-local}
mkdir -p "$out"
export DATAX_REQUIRE_BROWSER=1 DATAX_REQUIRE_PGDUMP=1

failed=()
step() {
	local name=$1
	shift
	echo "== $name"
	if "$@" >"$out/$name.log" 2>&1; then
		echo "   ok"
	else
		echo "   FAILED (see $out/$name.log)"
		failed+=("$name")
	fi
}

lint() {
	local fmt
	fmt=$(gofmt -l -s .)
	if [ -n "$fmt" ]; then
		echo "gofmt needed on:"
		echo "$fmt"
		return 1
	fi
	go vet ./... && GOTOOLCHAIN=$(go env GOVERSION) go run honnef.co/go/tools/cmd/staticcheck@2025.1.1 ./...
}

unit() {
	# shellcheck disable=SC2046 # the package list is meant to split
	go test -race -timeout 30m $(go list ./... | grep -v '/pkg/testutils/testcluster$')
}

# The four shards of ci.yaml's cluster job: every test runs in exactly one.
cluster_ac() { go test -race -timeout 30m -run '^Test[A-C]' ./pkg/testutils/testcluster/; }
cluster_dl() { go test -race -timeout 30m -run '^Test[D-L]' ./pkg/testutils/testcluster/; }
cluster_mr() { go test -race -timeout 30m -run '^Test[M-R]' ./pkg/testutils/testcluster/; }
cluster_sz() { go test -race -timeout 30m -skip '^Test[A-R]' ./pkg/testutils/testcluster/; }
cluster_all() {
	step cluster-a-c cluster_ac
	step cluster-d-l cluster_dl
	step cluster-m-r cluster_mr
	step cluster-s-z cluster_sz
}

vulncheck() { go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 -show verbose ./...; }

what=${1:-all}
case $what in
all)
	step lint lint
	step unit unit
	cluster_all
	;;
lint) step lint lint ;;
unit) step unit unit ;;
cluster)
	cluster_all
	;;
vulncheck) step vulncheck vulncheck ;;
*)
	echo "usage: $0 [all|lint|unit|cluster|vulncheck]" >&2
	exit 2
	;;
esac

if [ ${#failed[@]} -gt 0 ]; then
	echo
	echo "failed: ${failed[*]}"
	for f in "${failed[@]}"; do
		echo "---- $f"
		grep -E -B5 -A40 -- '--- FAIL|^panic:|WARNING: DATA RACE|test timed out|gofmt needed' "$out/$f.log" | tail -n 300 || tail -n 50 "$out/$f.log"
	done
	exit 1
fi
echo
echo "all steps passed"
