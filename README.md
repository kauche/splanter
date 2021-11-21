# splanter

A CLI tool which loads data from yaml files into the `Google Cloud Spanner` tables (mainly for the development).

## Usage

```
$  splanter \
     --project <GCP project ID> \
     --instance <Spanner instance name> \
     --database <Spanner database name> \
     --directory <Path to Directory which contains yaml files>
```

-   Create `<Spanner Table Name>.yaml` into the directory specified by `--directory`.
    -   Each field name must be the column name of the Spanner table.
