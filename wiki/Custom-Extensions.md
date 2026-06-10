# Custom extensions

Built-in parsers, transformers, and enrichers are registered in package `init()` functions. To add your own, register factories and build a **custom binary** — the default `./cmd/log-forwarder` entrypoint only includes built-ins.

The full working example lives in [`examples/custom/main.go`](https://github.com/sanjuthomas/log-forwarder/blob/main/examples/custom/main.go).

## Custom transformer

1. Implement the `transform.Transformer` interface:

```go
type Transformer interface {
    Transform(line []byte) (transform.Record, error)
}
```

2. Register a factory in `init()`:

```go
func init() {
    transform.Register("uppercase_tab", func(cfg config.TransformConfig) (transform.Transformer, error) {
        base, err := transform.New(config.TransformConfig{
            Type:    "tab",
            Columns: cfg.Columns,
        })
        if err != nil {
            return nil, err
        }
        return &uppercaseTab{base: base}, nil
    })
}
```

3. Wrap or replace behavior in your type:

```go
type uppercaseTab struct {
    base transform.Transformer
}

func (u *uppercaseTab) Transform(line []byte) (transform.Record, error) {
    record, err := u.base.Transform(line)
    if err != nil {
        return nil, err
    }
    if msg, ok := record["message"].(string); ok {
        record["message"] = strings.ToUpper(msg)
    }
    return record, nil
}
```

4. Reference the type in config:

```yaml
transform:
  type: uppercase_tab
  columns:
    - timestamp
    - level
    - message
```

The factory receives the full `TransformConfig`, so custom transformers can read `columns`, `pattern`, and `on_error` like built-ins.

## Custom parser

1. Implement the `parser.Parser` interface:

```go
type Parser interface {
    Feed(event watcher.LineEvent) ([]parser.Event, error)
    Flush() ([]parser.Event, error)
}
```

2. Register a factory in `init()`:

```go
func init() {
    parser.Register("my_parser", func(cfg config.ParserConfig) (parser.Parser, error) {
        return &myParser{}, nil
    })
}
```

3. Reference the type in config:

```yaml
parser:
  type: my_parser
```

## Custom enricher

1. Implement the `enrich.Enricher` interface:

```go
type Enricher interface {
    Enrich(record transform.Record) transform.Record
}
```

2. Register a factory in `init()`:

```go
func init() {
    enrich.Register("region", func(cfg config.EnricherConfig) (enrich.Enricher, error) {
        region := cfg.Fields["region"]
        if region == "" {
            region = "unknown"
        }
        return &regionEnricher{region: region}, nil
    })
}
```

3. Implement enrichment (mutate and return the record):

```go
type regionEnricher struct {
    region string
}

func (r *regionEnricher) Enrich(record transform.Record) transform.Record {
    record["region"] = r.region
    return record
}
```

4. Add to config:

```yaml
enrichers:
  - type: region
    fields:
      region: us-east-1
```

Use `fields` to pass arbitrary string configuration into your enricher factory.

## Custom filter

1. Implement the `filter.Predicate` interface:

```go
type Predicate interface {
    Match(record transform.Record) bool
}
```

2. Register a factory in `init()`:

```go
func init() {
    filter.Register("min_level", func(cfg config.FilterRuleConfig) (filter.Predicate, error) {
        return minLevelFilter{min: cfg.Value}, nil
    })
}
```

3. Reference the type in config:

```yaml
filter:
  match: all
  rules:
    - type: min_level
      value: ERROR
```

The factory receives the full `FilterRuleConfig`, so custom filters can read standard fields (`value`, `values`, `field`, …) or you can extend validation for your registered type.

## Custom sink

1. Implement the `sink.Sink` interface:

```go
type Sink interface {
    Publish(ctx context.Context, payload []byte) error
    Close() error
}
```

2. Optionally implement `sink.Checker` for a startup connectivity probe:

```go
type Checker interface {
    Check(ctx context.Context) error
}
```

3. Register a factory in `init()`:

```go
func init() {
    sink.Register("bigquery", func(cfg config.SinkConfig) (sink.Sink, error) {
        project, _ := cfg.Options["project"].(string)
        dataset, _ := cfg.Options["dataset"].(string)
        return newBigQuerySink(project, dataset)
    })
}
```

4. Reference the type in config and pass options:

```yaml
sink:
  type: bigquery
  options:
    project: my-gcp-project
    dataset: application_logs
```

The factory receives the full `SinkConfig`, so custom sinks can read `options` and ignore built-in `kafka` / `file` / `http_noauth` blocks.

## Build and run the custom binary

```bash
go build -o bin/log-forwarder-custom ./examples/custom
./bin/log-forwarder-custom -config configs/example.yaml
```

Copy [`examples/custom/main.go`](https://github.com/sanjuthomas/log-forwarder/blob/main/examples/custom/main.go) as a starting point for your own entrypoint — it mirrors `cmd/log-forwarder/main.go` but registers custom types before starting the pipeline.
