package postgres

import (
	"database/sql"
	"github.com/pkg/errors"
)

func ensurePostgreSQLExtentionAvailable(db *sql.DB, extentions []string) error {
	if nil == extentions {
		return nil
	}
	for _, extention := range extentions {
		row := db.QueryRow("SELECT EXISTS(SELECT * FROM pg_extension WHERE extname = $1)", extention)
		var exist bool
		if err := row.Scan(&exist); err != nil {
			return errors.Wrapf(err, "scan results of search for %s", extention)
		}

		if !exist {
			if _, err := db.Exec("CREATE EXTENTION $1", extention); err != nil {
				return errors.Wrapf(err, "creating extention %s", extention)
			}
		}
	}
	return nil
}
