package postgres

import (
	"database/sql"
	"fmt"
	"regexp"

	"github.com/pkg/errors"
)

func DropDatabase(config Configuration) error {
	db, err := Connect(Configuration{
		Address:  config.Address,
		Port:     config.Port,
		User:     config.User,
		Password: config.Password,
		Database: "postgres",
	})
	if err != nil {
		return err
	}
	defer db.Close()

	exist, err := databaseExist(db, config.Database)
	if nil != err {
		return err
	}

	if exist {
		_, err = db.Exec(fmt.Sprintf("DROP DATABASE %s", config.Database))
		if err != nil {
			return errors.Wrapf(err, "dropping database %s", config.Database)
		}
	}
	return nil
}

func initDatabase(config Configuration) error {
	db, err := Connect(Configuration{
		Address:  config.Address,
		Port:     config.Port,
		User:     config.User,
		Password: config.Password,
		Database: "postgres",
	})
	if err != nil {
		return err
	}
	defer db.Close()

	exist, err := databaseExist(db, config.Database)
	if nil != err {
		return err
	}

	if !exist {
		regexPattern := "^[a-z0-9_]*$"
		regex := regexp.MustCompile(regexPattern)
		if !regex.MatchString(config.Database) {
			return fmt.Errorf("invalid database name '%s' (Should match `%s`)", config.Database, regexPattern)
		}
		if !regex.MatchString(config.User) {
			return fmt.Errorf("invalid username '%s' (Should match `%s`)", config.User, regexPattern)
		}

		_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s WITH OWNER = %s ENCODING = 'UTF8' CONNECTION LIMIT = -1", config.Database, config.User))
		if err != nil {
			return errors.Wrapf(err, "creating database %s with owner %s", config.Database, config.User)
		}
	}
	return nil
}

func databaseExist(db *sql.DB, database string) (bool, error) {
	row := db.QueryRow("SELECT EXISTS(SELECT * FROM pg_database WHERE datname=$1)", database)
	var exist bool
	if err := row.Scan(&exist); err != nil {
		return false, errors.Wrapf(err, "scan results of search for %s", database)
	}
	return exist, nil
}
