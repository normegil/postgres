package postgres

import (
	"database/sql"
	"time"

	"testing"

	"context"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/normegil/docker"
	"github.com/normegil/interval"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

type ContainerConfiguration struct {
	Identifier string
}

func DockerDeploy(prefix string, ports interval.IntervalInteger) (Configuration, ContainerConfiguration, error) {
	binding := docker.PortBinding{Protocol: "TCP", Internal: 5432, ExternalInterval: ports.String()}
	pass, err := uuid.NewV4()
	if err != nil {
		return Configuration{}, ContainerConfiguration{}, errors.Wrapf(err, "Generating uuid")
	}
	info, _, err := docker.New(docker.Options{
		Name:  prefix,
		Image: "postgres",
		Ports: []docker.PortBinding{binding},
		EnvironmentVariables: map[string]string{
			"POSTGRES_PASSWORD": pass.String(),
		},
	})
	if err != nil {
		return Configuration{}, ContainerConfiguration{}, errors.Wrapf(err, "deploying postgres installation")
	}

	cfg := Configuration{
		Address:  info.Address.String(),
		Port:     info.Ports[binding],
		User:     "postgres",
		Password: pass.String(),
		Database: "postgres",
	}
	db, err := Connect(cfg)
	if err != nil {
		defer MustRemoveContainer(info.Identifier)
		return Configuration{}, ContainerConfiguration{}, errors.Wrapf(err, "attempt to connect to the newly created DB")
	}
	defer db.Close()

	timeout := time.Now().Add(5 * time.Second)
	if err = waitForPostgres(db, timeout); nil != err {
		defer MustRemoveContainer(info.Identifier)
		return Configuration{}, ContainerConfiguration{}, errors.Wrapf(err, "waiting for postgres to initialize")
	}

	return Configuration{
			Address:  cfg.Address,
			Port:     cfg.Port,
			User:     cfg.User,
			Password: cfg.Password,
		}, ContainerConfiguration{
			Identifier: info.Identifier,
		}, nil
}

func waitForPostgres(db *sql.DB, timeout time.Time) error {
	var err error
	for time.Now().Before(timeout) {
		if err = db.Ping(); nil == err {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.Wrapf(err, "timeout while waiting for database to come up. Last error")
}

func RemoveContainer(id string) error {
	dockerCli, err := client.NewEnvClient()
	if err != nil {
		return errors.Wrapf(err, "creating docker client")
	}

	if err := dockerCli.ContainerRemove(context.Background(), id, types.ContainerRemoveOptions{Force: true}); nil != err {
		return errors.Wrap(err, "removing container "+id)
	}
	return nil
}

func MustRemoveContainer(id string) {
	if err := RemoveContainer(id); nil != err {
		panic(err)
	}
}

func Test_RemoveContainer(t testing.TB, id string) {
	if err := RemoveContainer(id); nil != err {
		t.Fatalf(err.Error())
	}
}

func Test_Deploy(t testing.TB) (Configuration, ContainerConfiguration) {
	i := *interval.MustParseIntervalInteger("[15432;15440]")
	cfg, containerCfg, err := DockerDeploy("test-postgres", i)
	if err != nil {
		t.Fatalf("could not deploy postgres database: %s", err.Error())
	}
	return cfg, containerCfg
}
