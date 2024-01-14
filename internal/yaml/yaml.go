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

func assertTypedSlice[T any](slice []any) ([]T, error) {
	tSlice := make([]T, 0, len(slice))
	for _, v := range slice {
		tVal, ok := v.(T)
		if !ok {
			return nil, fmt.Errorf("unsupported mixed types list: %v", slice)
		}
		tSlice = append(tSlice, tVal)
	}
	return tSlice, nil
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

					// Spanner does not support the type uint64
					val, ok := p.Value.(uint64)
					if ok {
						records[i].Values[key] = int64(val)
						continue
					}

					// Spanner does not support the type []any
					list, ok := p.Value.([]any)
					if ok {
						switch list[0].(type) {
						case bool:
							records[i].Values[key], err = assertTypedSlice[bool](list)
							if err != nil {
								return err
							}
						case string:
							records[i].Values[key], err = assertTypedSlice[string](list)
							if err != nil {
								return err
							}
						case float64:
							records[i].Values[key], err = assertTypedSlice[float64](list)
							if err != nil {
								return err
							}
						case uint64:
							// Spanner does not support the type uint64 so assert to int64
							int64List := make([]int64, 0, len(list))
							for _, v := range list {
								n, ok := v.(uint64)
								if !ok {
									return fmt.Errorf("unsupported mixed types list: %v", v)
								}
								int64List = append(int64List, int64(n))
							}
							records[i].Values[key] = int64List
						default:
							return fmt.Errorf("unsupported type in list: %v", list)
						}
						continue
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
