---
document_type: development_environment
status: active
updated_at: 2026-09-19
---

# Development environment

## Назначение

Документ фиксирует воспроизводимые правила работы с toolchain в development/runtime-средах проекта.

Он не утверждает, что конкретный ChatGPT Web container, локальный host или CI runner всегда имеет установленный toolchain. Перед использованием версия проверяется фактически.

## Go baseline

Канонический implementation baseline определяется ADR-0001:

- Go 1.27.x;
- repository `go.mod` использует `go 1.27.0`;
- для exact validation предпочтителен текущий patch release линии 1.27, если он явно зафиксирован validation record или CI.

На 2026-09-16 R2 был проверен на Go 1.27.1.

## ChatGPT Web runtime

ChatGPT Web execution environment может содержать более старый системный Go и может не иметь outbound network access. Поэтому нельзя:

- считать системный `go` соответствующим repository baseline без `go version`;
- понижать repository Go requirement только ради конкретного Web container;
- заменять системный Go без необходимости;
- объявлять exact-toolchain validation выполненным, если проверка фактически прошла на другой версии.

### Предпочтительный fallback без сети

Если exact Go toolchain отсутствует, но пользователь может загрузить официальный Go archive в текущий диалог:

1. проверить имя/version/platform архива;
2. сверить SHA-256 с официальным значением с `go.dev/dl/`;
3. распаковать toolchain side-by-side, не удаляя системный Go;
4. использовать явный `PATH` для команд проверки;
5. зафиксировать фактический `go version` вместе с результатами validation.

Рекомендуемый ephemeral install path для Linux Web runtime:

```text
/opt/go<VERSION>
```

Это **не persisted project state**. В новом диалоге/container путь может отсутствовать, поэтому всегда сначала проверять:

```bash
/opt/go1.27.1/bin/go version
```

или искать доступный exact toolchain другим read-only способом.

### Go 1.27.1 archive used for R2 validation

Для validation 2026-09-16 пользователь загрузил официальный:

```text
go1.27.1.linux-amd64.tar.gz
```

Проверенный SHA-256:

```text
63d339f0da5ab53635a56f2490a7984dfe12dfcff22ad749f63edaf590168445
```

Toolchain был установлен side-by-side в `/opt/go1.27.1`. Системная версия Go не заменялась.

Этот путь описывает конкретный validation runtime и не является гарантией для последующих ChatGPT Web sessions.

## Required local validation for current core

При изменении Go core как минимум выполнять на target baseline:

```bash
go test ./...
go vet ./...
go test -race ./...
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...
gofmt -l internal
```

`gofmt -l` должен вернуть пустой вывод.

Для R3 с SQLite набор расширяется persistence/restart/migration tests и platform smoke по ADR-0002.

## CI

GitHub CI остаётся предпочтительным постоянным доказательством exact-toolchain/platform compatibility после его введения.

Локальный/Web validation дополняет CI, но не должен подменять его, если workflow уже является required project check.

До появления CI нельзя ссылаться на несуществующий workflow как на verification evidence.

## Постоянные проверки Go CI (#9)

Workflow: [.github/workflows/go-ci.yml](../.github/workflows/go-ci.yml).

| Check name | Среда | Проверки |
| --- | --- | --- |
| test (ubuntu-24.04) | Linux amd64, CGO_ENABLED=0 | go test -count=1 -timeout=5m ./..., go vet ./..., go build ./... |
| test (windows-2022) | Windows amd64, CGO_ENABLED=0 | Те же команды выполняются непосредственно на Windows |
| race and format (linux) | Linux amd64, CGO_ENABLED=1 для race instrumentation | gofmt -l . должен быть пустым; go test -race -count=1 -timeout=5m ./... |

Go закреплён на 1.27.1; GOTOOLCHAIN=local предотвращает неявную подмену toolchain, GOFLAGS=-mod=readonly — изменение module graph во время проверки. Обновление версии Go — отдельное проверяемое изменение. Cache отключён для минимального набора без внешних модулей; добавление зависимости R3 требует повторной проверки.

События: pull_request в main и push в main. На PR checkout выполняется по точному head SHA; это не тест синтетического merge commit. После слияния push-run проверяет фактический main. Actions закреплены commit SHA; credentials не сохраняются, permissions — contents: read. Каждый job ограничен 15 минутами, каждая test-команда — 5 минутами на пакет; новая проверка той же ветки отменяет устаревший запуск.

Перед слиянием изменений Go/CI нужны три успешных check на актуальном head. Это правило проекта; наличие branch-protection/ruleset этим документом не утверждается и не настраивается workflow. Пропущенный/отменённый job не является pass. После слияния проверить push-run main, при неуспехе остановить переход к следующему этапу.

CI не требует секретов, не разворачивает сервисы и не выполняет production mutations. Он не доказывает crash durability SQLite, пока R3 не добавит соответствующие тесты. Linux race instrumentation не вводит cgo dependency в продукт.
