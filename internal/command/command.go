package command

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/kauche/splanter/internal/spanner"
	"github.com/kauche/splanter/internal/yaml"
)

func Exec() {
	os.Exit(exec(context.Background()))
}

func exec(ctx context.Context) int {
	project := flag.String("project", "", "GCP Project ID")
	instance := flag.String("instance", "", "Spanner Instance Name")
	database := flag.String("database", "", "Spanner Database Name")
	directory := flag.String("directory", "", "Directory contains yaml files")

	flag.Parse()

	if *project == "" {
		fmt.Fprint(os.Stderr, "must specify --project")
		return 1
	}

	if *instance == "" {
		fmt.Fprint(os.Stderr, "must specify --instance")
		return 1
	}

	if *database == "" {
		fmt.Fprint(os.Stderr, "must specify --database")
		return 1
	}

	if *directory == "" {
		fmt.Fprint(os.Stderr, "must specify --directory")
		return 1
	}

	loader := yaml.NewLoader()
	tables, err := loader.Load(ctx, *directory)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load yaml files: %s", err.Error())
		return 1
	}

	db, err := spanner.NewDB(ctx, *project, *instance, *database)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to spanner: %s", err.Error())
		return 1
	}

	if err := db.Save(ctx, tables); err != nil {
		fmt.Fprintf(os.Stderr, "failed to load data to spanner tables: %s", err.Error())
		return 1
	}

	return 0
}
