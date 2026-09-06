package parser

import "testing"

// Placement grammar (issue #176).

func TestParseCreateDatabaseWithPlacement(t *testing.T) {
	stmts, err := Parse(`CREATE DATABASE eu WITH (replicas = 5, constraints = ('region=eu-west-1', 'region=eu-north-1'))`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cd, ok := stmts[0].(*CreateDatabase)
	if !ok {
		t.Fatalf("got %T, want *CreateDatabase", stmts[0])
	}
	if cd.Name != "eu" || cd.Placement == nil {
		t.Fatalf("name %q placement %+v", cd.Name, cd.Placement)
	}
	if !cd.Placement.SetReplicas || cd.Placement.Replicas != 5 {
		t.Fatalf("replicas: %+v", cd.Placement)
	}
	if got, want := len(cd.Placement.Constraints), 2; got != want {
		t.Fatalf("constraints %v", cd.Placement.Constraints)
	}
	if cd.Placement.Constraints[0] != "region=eu-west-1" {
		t.Fatalf("constraint %q", cd.Placement.Constraints[0])
	}
}

// WITH is optional and either option may stand alone.
func TestParsePlacementShorthands(t *testing.T) {
	stmts, err := Parse(`CREATE DATABASE a (constraints = 'region=eu'); CREATE DATABASE b WITH (replicas = 3)`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	a := stmts[0].(*CreateDatabase)
	if a.Placement == nil || a.Placement.SetReplicas || len(a.Placement.Constraints) != 1 {
		t.Fatalf("a: %+v", a.Placement)
	}
	b := stmts[1].(*CreateDatabase)
	if b.Placement == nil || b.Placement.SetConstraints || b.Placement.Replicas != 3 {
		t.Fatalf("b: %+v", b.Placement)
	}
}

func TestParseAlterDatabaseSetPlacement(t *testing.T) {
	stmts, err := Parse(`ALTER DATABASE eu SET (constraints = ())`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ad, ok := stmts[0].(*AlterDatabase)
	if !ok {
		t.Fatalf("got %T", stmts[0])
	}
	if ad.Name != "eu" || ad.NewName != "" || ad.Placement == nil {
		t.Fatalf("%+v", ad)
	}
	// An empty list is how a constraint is lifted: named, but nothing in it.
	if !ad.Placement.SetConstraints || len(ad.Placement.Constraints) != 0 {
		t.Fatalf("placement %+v", ad.Placement)
	}
	// RENAME still parses to the same node.
	stmts, err = Parse(`ALTER DATABASE eu RENAME TO emea`)
	if err != nil {
		t.Fatalf("parse rename: %v", err)
	}
	if ad := stmts[0].(*AlterDatabase); ad.NewName != "emea" || ad.Placement != nil {
		t.Fatalf("rename: %+v", ad)
	}
}

// The parentheses around the option list are optional, so the spelling
// in issue #176 parses too.
func TestParsePlacementWithoutParentheses(t *testing.T) {
	stmts, err := Parse(`CREATE DATABASE eu WITH REPLICAS = 3, CONSTRAINTS = ('region=eu-west-1'); ` +
		`ALTER DATABASE eu SET CONSTRAINTS = ('region=eu-west-1', 'region=eu-central-1'); ` +
		`ALTER DATABASE eu SET REPLICAS = 5`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cd := stmts[0].(*CreateDatabase)
	if cd.Placement == nil || cd.Placement.Replicas != 3 || len(cd.Placement.Constraints) != 1 {
		t.Fatalf("create: %+v", cd.Placement)
	}
	a1 := stmts[1].(*AlterDatabase)
	if a1.Placement == nil || a1.Placement.SetReplicas || len(a1.Placement.Constraints) != 2 {
		t.Fatalf("alter constraints: %+v", a1.Placement)
	}
	a2 := stmts[2].(*AlterDatabase)
	if a2.Placement == nil || a2.Placement.Replicas != 5 || a2.Placement.SetConstraints {
		t.Fatalf("alter replicas: %+v", a2.Placement)
	}
}

func TestParseShowPlacement(t *testing.T) {
	stmts, err := Parse(`SHOW PLACEMENT; SHOW PLACEMENT FOR DATABASE eu`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if sp := stmts[0].(*ShowPlacement); sp.Database != "" {
		t.Fatalf("bare: %+v", sp)
	}
	if sp := stmts[1].(*ShowPlacement); sp.Database != "eu" {
		t.Fatalf("for database: %+v", sp)
	}
}

func TestParsePlacementErrors(t *testing.T) {
	for _, q := range []string{
		`CREATE DATABASE a WITH ()`,
		`CREATE DATABASE a WITH (replicas = 'three')`,
		`CREATE DATABASE a WITH (regions = 'eu')`,
		`CREATE DATABASE a WITH (replicas = 3, replicas = 5)`,
		`CREATE DATABASE a WITH (constraints = (region=eu))`,
		`ALTER DATABASE a SET`,
	} {
		if _, err := Parse(q); err == nil {
			t.Fatalf("%q parsed, want an error", q)
		}
	}
}
