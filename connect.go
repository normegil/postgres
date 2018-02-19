package postgres

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"

	"github.com/pkg/errors"
)

const DRIVER_NAME = "postgres"
const CONNECTION_STRING = "host=%s port=%d user=%s password=%s dbname=%s sslmode=disable"

func New(cfg Configuration) (*sql.DB, error) {
	if err := initDatabase(cfg); nil != err {
		return nil, errors.Wrapf(err, "initializing database")
	}

	return Connect(cfg)
}

func Connect(cfg Configuration) (*sql.DB, error) {
	if "" == cfg.Database {
		return nil, fmt.Errorf("cannot connect to unspecified database")
	}

	connectionInfo := fmt.Sprintf(CONNECTION_STRING, cfg.Address, cfg.Port, cfg.User, cfg.Password, cfg.Database)
	db, err := sql.Open(DRIVER_NAME, connectionInfo)
	if err != nil {
		return nil, errors.Wrapf(err, "connect to postgres (%s)", connectionInfo)
	}
	return db, nil
}

type Configuration struct {
	Address  string `toml:"address"`
	Port     int    `toml:"port"`
	User     string `toml:"user"`
	Password string `toml:"pass"`
	Database string `toml:"database"`
}
