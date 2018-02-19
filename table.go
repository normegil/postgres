package postgres

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

type TableInfos struct {
	Queries map[string]string
	Owner   string
}

func CreateTable(db *sql.DB, infos TableInfos) error {
	existenceQuery, found := infos.Queries["Table-Existence"]
	if !found {
		return fmt.Errorf("A query should exist under the 'Table-Existence' key")
	}
	row := db.QueryRow(existenceQuery)
	var exist bool
	err := row.Scan(&exist)
	if err != nil {
		return errors.Wrapf(err, "scan table existence result")
	}

	if !exist {
		createQuery, found := infos.Queries["Table-Create"]
		if !found {
			return fmt.Errorf("A query should exist under the 'Table-Create' key")
		}
		_, err := db.Exec(createQuery)
		if err != nil {
			return errors.Wrapf(err, "creating table in database")
		}
		ownerParametrizedQuery, found := infos.Queries["Table-Set-Owner"]
		if !found {
			return fmt.Errorf("A query should exist under the 'Table-Set-Owner' key")
		}
		ownerQuery := strings.Replace(ownerParametrizedQuery, "$1", "%s", -1)
		_, err = db.Exec(fmt.Sprintf(ownerQuery, infos.Owner))
		if err != nil {
			return errors.Wrapf(err, "set table owner to %s", infos.Owner)
		}
	}
	return nil
}
