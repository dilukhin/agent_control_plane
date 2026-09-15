---
document_type: validation_record
component: R2_reference_core
status: passed
validated_at: 2026-09-16
toolchain: go1.27.1
platform: linux/amd64
---

# R2 exact Go 1.27.1 validation

## Scope

Проверена реализация R2, слитая в `main` через PR #5:

- protocol validation;
- state machine;
- in-memory core store;
- evidence model;
- duplicate-message handling;
- unknown-outcome/retry-safety semantics;
- concurrency/ownership tests.

Validation выполнялась на scratch-копии Go source/test files, ранее сверенных по Git blob SHA с PR #5. `go.mod` был приведён к repository value:

```text
go 1.27.0
```

## Toolchain

Фактическая версия:

```text
go version go1.27.1 linux/amd64
```

Official Linux amd64 archive:

```text
go1.27.1.linux-amd64.tar.gz
```

SHA-256:

```text
63d339f0da5ab53635a56f2490a7984dfe12dfcff22ad749f63edaf590168445
```

Toolchain был установлен side-by-side в ephemeral ChatGPT Web runtime; системный Go не заменялся.

## Results

Passed:

```bash
go test ./...
go vet ./...
go test -race ./...
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...
gofmt -l internal
```

`gofmt -l internal` вернул пустой вывод.

Во время первого объединённого запуска внешний execution timeout сработал во время Windows cross-build. Это не было compiler/test failure. Windows и Linux cross-build были затем запущены отдельно и завершились успешно.

## Result

Предыдущий R2 validation gap «exact target-toolchain smoke pending» закрыт.

Это не доказывает будущую совместимость R3/SQLite: после добавления `modernc.org/sqlite` exact-toolchain и Windows/Linux validation выполняются повторно для нового dependency graph.
