package postgres

import (
	"fmt"
	"regexp"

	"github.com/pkg/errors"
)

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

	row := db.QueryRow("SELECT EXISTS(SELECT * FROM pg_database WHERE datname=$1)", config.Database)
	if err != nil {
		return errors.Wrapf(err, "searching for database %s", config.Database)
	}
	var exist bool
	if err = row.Scan(&exist); err != nil {
		return errors.Wrapf(err, "scan results of search for %s", config.Database)
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
