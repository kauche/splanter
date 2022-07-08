package yaml

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/goccy/go-yaml"

	"github.com/kauche/splanter/internal/model"
)

type Loader struct{}

func NewLoader() *Loader {
	return &Loader{}
}

func (l *Loader) Load(ctx context.Context, dir string) ([]*model.Table, error) {
	var tables []*model.Table
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		fname := entry.Name()
		ext := filepath.Ext(fname)
		if !entry.IsDir() && (ext == ".yaml" || ext == ".yml") {
			var items []yaml.MapSlice
			file, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("failed to open yaml file: %w", err)
			}
			defer file.Close()

			seeds, err := io.ReadAll(file)
			if err != nil {
				return fmt.Errorf("failed to read yaml file: %w", err)
			}

			if err = yaml.Unmarshal(seeds, &items); err != nil {
				return fmt.Errorf("failed to unmarshal yaml file: %w", err)
			}

			records := make([]*model.Record, len(items))
			for i, proparties := range items {
				records[i] = &model.Record{
					Values: make(map[string]interface{}),
				}

				for _, p := range proparties {
					key, ok := p.Key.(string)
					if !ok {
						return fmt.Errorf("failed to unmarshal yaml proparty: %w", err)
					}

					records[i].Values[key] = p.Value
				}

			}

			name := filepath.Base(strings.TrimSuffix(fname, ext))
			tables = append(tables, &model.Table{
				Name:    name,
				Records: records,
			})
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to walk dir %s: %w", dir, err)
	}

	return tables, nil
}
