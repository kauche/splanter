package command

import (
	"context"
	"flag"
	"os"

	"github.com/110y/splanter/internal/spanner"
	"github.com/110y/splanter/internal/yaml"
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
		return 1
	}

	if *instance == "" {
		return 1
	}

	if *database == "" {
		return 1
	}

	if *directory == "" {
		return 1
	}

	loader := yaml.NewLoader()
	tables, err := loader.Load(ctx, *directory)
	if err != nil {
		return 1
	}

	db, err := spanner.NewDB(ctx, *project, *instance, *database)
	if err != nil {
		return 1
	}

	if err := db.Save(ctx, tables); err != nil {
		return 1
	}

	return 0
}
