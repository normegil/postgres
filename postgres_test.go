package postgres_test

import (
	"database/sql"
	"testing"

	"fmt"
	"github.com/normegil/interval"
	"github.com/normegil/postgres"
)

func TestPostgresPackage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skip postgres docker tests: short mode")
	}

	t.Run("Can Deploy", func(t *testing.T) {
		intervalInteger := interval.MustParseIntervalInteger("[11000;11200]")
		cfg, container, err := postgres.DockerDeploy("TestPostgresDeploy", *intervalInteger)
		if nil != err {
			t.Fatalf(err.Error())
		}
		defer postgres.Test_RemoveContainer(t, container.Identifier)

		cfg.Database = "postgres"
		db, err := postgres.Connect(cfg)
		if err != nil {
			t.Fatalf("connect to postgres (%+v): %s", cfg, err.Error())
		}
		defer db.Close()

		const databaseName = "postgres"
		row := db.QueryRow("SELECT EXISTS(SELECT * FROM pg_database WHERE datname=$1)", databaseName)
		var found bool
		if err := row.Scan(&found); err != nil {
			t.Fatalf("reading row: %s", err.Error())
		}

		if !found {
			t.Errorf("database deployed and initialized without %s", databaseName)
		}
	})

	t.Run("New", func(t *testing.T) {
		cfg, container := postgres.Test_Deploy(t)
		defer postgres.Test_RemoveContainer(t, container.Identifier)

		t.Run("Create Database if doesn't exist", func(t *testing.T) {
			testcases := []string{
				"wiki",
				"wi_ki",
				"wi0",
			}
			for _, testdata := range testcases {
				t.Run(testdata, func(t *testing.T) {
					db, err := postgres.New(postgres.Configuration{
						Address:  cfg.Address,
						Port:     cfg.Port,
						User:     cfg.User,
						Password: cfg.Password,
						Database: testdata,
					})
					if err != nil {
						t.Fatalf("connecting to %+v: %s", cfg, err.Error())
					}
					defer db.Close()

					row := db.QueryRow("SELECT EXISTS(SELECT * FROM pg_database WHERE datname=$1)", testdata)
					var exist bool
					if err := row.Scan(&exist); nil != err {
						t.Fatalf("error while checking database %s existence: %s", testdata, err.Error())
					}
				})
			}
		})

		t.Run("Invalid database names", func(t *testing.T) {
			testcases := []string{
				"Wiki",
				"wi-ki",
				"wi$",
			}
			for _, testdata := range testcases {
				t.Run(testdata, func(t *testing.T) {
					db, err := postgres.New(postgres.Configuration{
						Address:  cfg.Address,
						Port:     cfg.Port,
						User:     cfg.User,
						Password: cfg.Password,
						Database: testdata,
					})
					if err == nil {
						defer db.Close()
						t.Fatalf("'%s': Invalid names should return errors", testdata)
					}
				})
			}
		})
	})

	cfg, container := postgres.Test_Deploy(t)
	defer postgres.Test_RemoveContainer(t, container.Identifier)
	cfg.Database = "testdatabase"
	db, err := postgres.New(cfg)
	if err != nil {
		t.Fatalf("connecting to %+v: %s", cfg, err.Error())
	}
	defer db.Close()

	const selectOwnerOfTable = "SELECT tableowner FROM pg_tables WHERE tablename=$1"
	t.Run("CreateTable: Table Exist", func(t *testing.T) {
		const testTableName = "existingtesttable"
		if _, err = db.Exec("CREATE TABLE public." + testTableName + " (id UUID NOT NULL) TABLESPACE pg_default;"); err != nil {
			t.Fatalf("creating table '%s': %s", testTableName, err.Error())
		}

		const expectedOwner = "testuser"
		if _, err := db.Exec("CREATE ROLE " + expectedOwner); nil != err {
			t.Fatalf("Create role '%s': %s", expectedOwner, err.Error())
		}
		defer func() {
			if _, err := db.Exec("DROP ROLE " + expectedOwner); nil != err {
				t.Fatalf("Could not drop %s: %s", expectedOwner, err.Error())
			}
		}()

		if _, err := db.Exec("ALTER TABLE public." + testTableName + " OWNER TO " + expectedOwner); err != nil {
			t.Fatalf("set owner of '%s' to '%s': %s", testTableName, expectedOwner, err.Error())
		}
		defer removeTable(t, db, testTableName)

		err = postgres.CreateTable(db, postgres.TableInfos{
			Queries: map[string]string{
				"Table-Existence": "SELECT CAST(1 AS BIT)",
			},
			Owner: "postgres",
		})
		if err != nil {
			t.Fatalf("create existing table: %s", err.Error())
		}

		newRow := db.QueryRow(selectOwnerOfTable, testTableName)
		var loadedOwner string
		if err := newRow.Scan(&loadedOwner); nil != err {
			t.Fatalf("scanning table owner: %s", err.Error())
		}

		if expectedOwner != loadedOwner {
			t.Errorf("Owner should not have changed (Expected:%s;Got:%s)", expectedOwner, loadedOwner)
		}
	})

	t.Run("CreateTable", func(t *testing.T) {
		testcases := []struct {
			Name  string
			Owner string
		}{
			{"test", "postgres"},
		}
		for _, testdata := range testcases {
			t.Run("Creating "+testdata.Name, func(t *testing.T) {
				tableExistenceQuery := "SELECT EXISTS(SELECT * FROM pg_tables WHERE tablename = '" + testdata.Name + "');"
				err := postgres.CreateTable(db, postgres.TableInfos{
					Queries: map[string]string{
						"Table-Existence": tableExistenceQuery,
						"Table-Create":    "CREATE TABLE public." + testdata.Name + " (id UUID NOT NULL) TABLESPACE pg_default;",
						"Table-Set-Owner": "ALTER TABLE public." + testdata.Name + " OWNER TO $1;",
					},
					Owner: testdata.Owner,
				})
				if err != nil {
					t.Fatalf("creating table %s: %s", testdata.Name, err.Error())
				}
				defer removeTable(t, db, testdata.Name)

				row := db.QueryRow(tableExistenceQuery)
				var exist bool
				if err := row.Scan(&exist); nil != err {
					t.Fatalf("check table %s existence: %s", testdata.Name, err.Error())
				}
				if !exist {
					t.Fatalf("table %s should exist but doesn't", testdata.Name)
				}

				ownerRow := db.QueryRow(selectOwnerOfTable, testdata.Name)
				var tableOwner string
				if err := ownerRow.Scan(&tableOwner); nil != err {
					t.Fatalf("select owner of %s: %s", testdata.Name, err.Error())
				}

				if tableOwner != testdata.Owner {
					t.Fatalf("wrong owner for %s (Expected:%s;Got:%s)", testdata.Name, testdata.Owner, tableOwner)
				}
			})
		}
	})
}

func removeTable(t testing.TB, db *sql.DB, tableName string) {
	if _, err := db.Exec(fmt.Sprintf("DROP TABLE %s", tableName)); nil != err {
		t.Fatalf("could not remove table %s: %s", tableName, err.Error())
	}
}
