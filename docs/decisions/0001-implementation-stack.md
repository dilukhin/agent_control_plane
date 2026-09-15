---
document_type: architecture_decision
decision_id: ADR-0001
status: accepted
date: 2026-09-15
owners:
  - agent_control_plane
---

# ADR-0001: implementation stack для reference core R2

## Решение

Для reference core R2 выбран **Go 1.27.x**.

Начальная реализация должна быть:

- pure Go без cgo в runtime core;
- с Go modules;
- со стандартной библиотекой как предпочтительным вариантом;
- без framework, daemon/server topology, database, transport SDK или provider SDK до соответствующих roadmap gates;
- с явными типами для protocol/state/evidence contracts;
- с детерминированным in-memory state repository для R2;
- с unit/state-transition/duplicate/unknown-outcome/concurrency tests.

Target module path:

`github.com/dilukhin/agent_control_plane`

## Контекст

Gate A открыт после завершения R1. Требования roadmap к стеку:

- Windows и Linux first-class;
- удобное выражение versioned protocol/state contracts;
- высокая testability state machine и concurrency;
- простая packaging/deployment модель;
- малый dependency footprint;
- пригодность для локальных workers;
- отсутствие преждевременной привязки к storage, transport или provider.

На 2026-09-15 актуальная stable-линейка Go — 1.27, текущий patch release — 1.27.1.

## Рассмотренные варианты

### Go

Плюсы для этого проекта:

- статическая типизация и простой compile-time contract surface;
- официальный toolchain для Windows и Linux;
- pure-Go executable собирается непосредственно `go build`;
- простая cross-compilation pure-Go binaries через `GOOS`/`GOARCH`;
- встроенные `go test`, race detector и native fuzzing;
- goroutines/channels и стандартные sync primitives подходят для проверки shared-state/concurrency semantics;
- минимальный runtime/deployment footprint: не требуется отдельный language runtime на target host для обычного compiled binary;
- можно долго оставаться stdlib-first.

Ограничения:

- race detector требует cgo; на Windows для `-race` требуется совместимый C toolchain;
- статическая типизация Go менее выразительна, чем Rust type system, поэтому часть state invariants всё равно должна проверяться state machine/tests.

### Rust

Плюсы:

- сильная статическая типизация;
- Windows/Linux входят в Tier 1 platform support;
- Cargo имеет хороший unit/integration test workflow;
- compiled binary и строгая memory/concurrency safety.

Минусы для первого reference core:

- выше implementation/maintenance complexity;
- Windows development обычно требует дополнительный MSVC/C++ build toolchain;
- для R2 преимущества memory-safety не компенсируют увеличение сложности небольшой state-machine codebase;
- slower iteration для инфраструктуры, где основной риск сейчас — protocol/state semantics, а не unsafe memory.

Rust остаётся допустимым будущим вариантом для отдельных performance/security-sensitive components, если появится измеримая потребность.

### Python

На 2026-09-15 latest stable feature series — Python 3.14, актуальный patch release 3.14.7; Python 3.15 ещё release candidate.

Плюсы:

- высокая скорость разработки;
- богатая стандартная библиотека и ecosystem;
- хороший Windows/Linux developer experience;
- удобен для adapters, automation и tooling.

Минусы для core:

- type hints не являются обязательной runtime/static guarantee без дополнительных инструментов;
- standalone deployment обычно требует отдельного Python runtime или стороннего packaging layer;
- cross-platform standalone packaging менее прямолинейно, чем `go build`;
- dependency/tooling footprint для строгой типизации/packaging выше.

Python остаётся хорошим кандидатом для вспомогательных tools/adapters, если contract boundary это позволяет.

### TypeScript + Node.js

На 2026-09-15 Node.js 24 находится в LTS; Node.js 26 — Current. TypeScript обеспечивает static type checking, но types стираются и runtime остаётся JavaScript/Node.

Плюсы:

- сильный developer experience для schema-heavy application code;
- удобная структурная типизация;
- хорошая cross-platform поддержка Node;
- вероятно удобен для будущего Web/UI слоя.

Минусы для core:

- требуется Node runtime либо дополнительный bundling/packaging layer;
- npm dependency surface обычно больше;
- TypeScript types не существуют runtime, поэтому protocol validation всё равно требует runtime schemas/validators;
- меньше выигрыша для небольшого transport-agnostic binary core по сравнению с Go.

TypeScript остаётся естественным кандидатом для UI/Web components, но не выбран для R2 core.

## Итог сравнения

| Критерий | Go | Rust | Python | TypeScript/Node |
|---|---|---|---|---|
| Windows/Linux | отлично | отлично | отлично | отлично |
| Compile-time contracts | хорошо | отлично | средне | хорошо |
| Concurrency/state tests | отлично | отлично | хорошо | хорошо |
| Простота binary deployment | отлично | хорошо | ниже | ниже |
| Минимальный dependency footprint | отлично | хорошо | хорошо/средне | средне |
| Скорость реализации R2 | хорошо | ниже | отлично | хорошо |
| Соответствие текущему риску проекта | **лучшее** | избыточно | слабее по deployment/contracts | слабее по deployment/runtime |

Главные риски R2 — корректность state machine, duplicate/retry/unknown-outcome semantics и concurrency. Go даёт достаточную статическую строгость и сильный встроенный test/concurrency toolchain без высокой цены Rust и без отдельного runtime/packaging слоя Python/Node.

## Constraints для R2

1. Использовать Go 1.27 language/toolchain baseline.
2. Runtime core не использует cgo.
3. Не добавлять third-party dependency без конкретной потребности, которую нельзя разумно закрыть stdlib.
4. Не добавлять database/ORM.
5. Не добавлять HTTP/gRPC/WebSocket/SSH/local-IPC transport.
6. Не добавлять model/provider SDK.
7. Не создавать daemon/service topology.
8. Не делать serialization format архитектурным владельцем state semantics: Go structs реализуют уже принятый R1 contract.
9. State transitions должны проходить через один проверяемый state-machine boundary.
10. Concurrency correctness проверяется tests; race detector используется там, где доступен toolchain.

## Минимальные проверки R2

Обязательный локальный набор:

- `go test ./...`;
- `go vet ./...`;
- `go test -race ./...` как минимум на поддерживаемой среде с необходимым C toolchain;
- targeted tests для valid/invalid transitions;
- duplicate delivery/idempotency semantics;
- unknown-outcome/reconciliation transitions;
- revision/CAS conflicts;
- overlapping `conflict_scope`;
- stale lease/generation;
- secret-field rejection/redaction boundaries;
- fuzzing protocol/state validators, когда появится parser/decoder boundary.

Windows и Linux должны запускать обычные tests/build. Отсутствие Windows C toolchain для race detector не делает Windows second-class: `-race` может выполняться на другой поддерживаемой платформе, а Windows получает обычные build/test/smoke checks.

## Предлагаемая начальная структура R2

Это implementation guidance, а не отдельный protocol contract:

```text
go.mod
internal/
  protocol/
  state/
  evidence/
  core/
```

Дополнительные packages создаются только при фактической необходимости.

На R2 не нужен `cmd/`, если reference core можно полноценно проверить package tests. CLI/daemon добавляется только по отдельной задаче.

## Consequences

Положительные:

- R2 можно начать без выбора storage/transport/provider;
- compiled artifacts естественно поддерживают Windows/Linux;
- state/concurrency tests имеют минимальный tooling overhead;
- deployment первого локального worker/core позже не требует Python/Node runtime.

Отрицательные:

- часть schema/runtime validation придётся написать явно;
- Go не кодирует все state-machine invariants type system;
- Windows race detector требует дополнительный C toolchain.

## Revisit conditions

Решение пересматривается только при evidence, что:

- обязательный будущий component требует ecosystem, существенно недоступный в Go;
- profiling показывает обоснованную необходимость другого runtime;
- provider/transport integration создаёт неприемлемую стоимость adapter boundary;
- cross-platform deployment фактически хуже ожидаемого;
- R2 implementation выявляет системную неспособность выразить/проверить принятые contracts.

Preference другого языка без такого evidence не является основанием для переписывания core.

## Источники

Проверены 2026-09-15:

- Go releases: https://go.dev/dl/
- Go release history/support: https://go.dev/doc/devel/release
- Go build command: https://pkg.go.dev/cmd/go
- Go cross-compilation: https://go.dev/wiki/WindowsCrossCompiling
- Go race detector: https://go.dev/doc/articles/race_detector
- Go fuzzing: https://go.dev/doc/security/fuzz/
- Rust platform support: https://doc.rust-lang.org/rustc/platform-support.html
- Rust installation: https://www.rust-lang.org/tools/install
- Cargo tests: https://doc.rust-lang.org/stable/cargo/guide/tests.html
- Python 3.14.7: https://www.python.org/downloads/release/python-3147/
- Python application deployment: https://packaging.python.org/en/latest/discussions/deploying-python-applications/
- Node.js releases: https://nodejs.org/en/about/previous-releases
- TypeScript Handbook: https://www.typescriptlang.org/docs/handbook/intro
