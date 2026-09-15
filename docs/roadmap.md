---
document_type: project_roadmap
version: 1.0
status: active
updated_at: 2026-09-15
---

# agent_control_plane: roadmap

## 1. Назначение roadmap

Этот документ задаёт последовательность проектирования и реализации `agent_control_plane` после foundation-этапа.

Roadmap не заменяет `docs/project_baseline.md`. Baseline остаётся владельцем устойчивых границ и инвариантов проекта. Roadmap определяет порядок получения проверяемых результатов и точки, в которых можно принимать ещё не принятые архитектурные решения.

Ключевой принцип: сначала определить общий control contract и модель состояния, затем строить runtime и adapters. Конкретный язык, storage, daemon topology, transport и provider SDK не выбираются без проверяемой потребности соответствующего этапа.

## 2. Текущее состояние

На старте roadmap:

- repository содержит только foundation-документы;
- product implementation отсутствует;
- tests и CI jobs отсутствуют;
- язык/framework/package manager не выбраны;
- concrete storage, daemon/client/server topology, transports, provider adapters и model router не выбраны;
- `docs/project_baseline.md` является каноническим pre-roadmap baseline.

Первый следующий результат — design package для control protocol и state model. Реализация до него считается преждевременной.

## 3. Сквозные критерии

Каждый этап должен сохранять следующие свойства:

- control protocol отделён от transport;
- task/operation имеют устойчивую identity, correlation и observable state;
- timeout/disconnect mutation означает unknown outcome до actual-state verification;
- retry mutation допускается только после анализа idempotency, ownership и фактического состояния;
- основная задача и ортогональная проблема процесса разделимы;
- escalation к worker/model/user является явным маршрутом;
- provider/worker-specific детали не проникают в общую orchestration logic;
- persisted protocol/state versioned и имеет compatibility/migration policy;
- queue/log/replay/evidence имеют bounded retention;
- secrets не попадают в обычные task payloads, logs, queues, receipts/evidence или Git;
- Windows и Linux остаются first-class локальными платформами.

## 4. Этап R1 — Control contracts

**Статус: выполнено 2026-09-15.** Приняты `docs/control_protocol.md`, `docs/state_model.md`, `docs/evidence_model.md` и `docs/security_model.md` версии 1.0.

### Цель

Зафиксировать логический контракт control plane до выбора runtime stack.

### Результаты

Создать документы-владельцы:

- `docs/control_protocol.md` — versioned envelope, task/operation identity, correlation, commands/events, delivery semantics и правила compatibility;
- `docs/state_model.md` — task/operation state machines, допустимые переходы, terminal/non-terminal states, ownership и unknown outcome;
- `docs/evidence_model.md` — receipts/evidence, различие claimed result и verified actual state, минимальные требования к verification;
- `docs/security_model.md` — trust boundaries, secret-handling, redaction и минимальные privilege assumptions.

### Обязательные сценарии

Design должен явно покрыть:

1. успешную bounded operation;
2. timeout до известного результата;
3. disconnect после потенциально выполненной mutation;
4. duplicate delivery/retry;
5. потерю worker после принятия задачи;
6. параллельные независимые tasks;
7. конкурирующие operations над одним target;
8. escalation к другому worker/model/user;
9. отделение основной задачи от watchdog/deferred issue;
10. protocol/state version mismatch.

### Acceptance criteria

- определены идентичности task и operation;
- есть таблица/диаграмма переходов состояния;
- unknown outcome является явным состоянием или явно моделируемым режимом;
- описана correlation/ownership model;
- определены требования к evidence;
- описаны backward/forward compatibility правила для первой protocol version;
- ни один transport/provider не встроен в общий контракт как обязательный.

## 5. Этап R2 — Minimal reference core

**Статус: выполнено и проверено.** Reference core покрывает protocol validation, operation state machine, revision/CAS, in-memory state/evidence/message store, attempt/lease ownership, mutation conflict scopes, duplicate-message handling и explicit unknown-outcome/retry-safety path. Exact validation на Go 1.27.1 выполнена 2026-09-16: tests/vet/race, Windows/Linux pure-Go cross-build и gofmt прошли; см. `docs/validation/r2-go-1.27.1.md`.

### Цель

Реализовать минимальное transport-agnostic ядро, проверяющее design R1.

### До начала

Отдельным архитектурным решением выбрать минимальный implementation stack. Выбор должен учитывать:

- Windows/Linux;
- простоту типизации protocol/state contracts;
- testability;
- packaging/deployment;
- стоимость поддержки локальных workers.

### Минимальная функциональность

- создание task/operation identity;
- validation protocol version;
- state transition engine;
- correlation/ownership checks;
- registration of evidence;
- explicit unknown-outcome path;
- in-memory reference repository/state store;
- deterministic API для unit tests.

### Проверки

- transition tests;
- invalid transition tests;
- duplicate/idempotency tests;
- unknown-outcome tests;
- concurrency tests для shared state;
- redaction tests для запрещённых secret-полей.

R2 не обязан иметь daemon, remote transport, provider SDK или durable database.

## 6. Этап R3 — Durable state, recovery и retention

### Цель

Сделать состояние переживающим restart и пригодным для reconciliation.

### Результаты

- выбрать storage только после требований R1/R2;
- persisted state version;
- migration policy и fixtures;
- restart/recovery path;
- operation ownership/lease semantics, если они необходимы;
- bounded retention policy для queue/log/replay/evidence;
- garbage collection с сохранением required audit/evidence minimum.

### Acceptance criteria

После контролируемого restart система должна однозначно различать:

- completed/verified work;
- known failure;
- active/recoverable work;
- unknown outcome, требующий reconciliation.

Повтор mutation после restart не должен происходить автоматически без проверки фактического состояния.

## 7. Этап R4 — Worker и provider boundaries

### Цель

Отделить общий control plane от конкретного агента и модели.

### Результаты

- worker contract: capabilities, accept/reject, lifecycle, heartbeat/lease при необходимости, result/evidence reporting;
- provider contract: model invocation boundary без vendor-specific полей в core;
- reference local worker adapter;
- mock/reference provider для contract tests;
- capability negotiation и explicit unsupported result.

### Acceptance criteria

Один и тот же core должен работать с двумя тестовыми workers/providers без изменения orchestration logic.

На этом этапе ещё не требуется полноценный model router или cost optimizer.

## 8. Этап R5 — Transport adapters и первый end-to-end путь

### Цель

Провести одну задачу через реальный transport, не передавая transport ownership workflow state.

### Кандидаты

- local IPC для локального worker;
- `ssh_relay` adapter для существующего remote transport contract;
- HTTPS/VPS relay как отдельный кандидат, если он нужен для фактического сценария.

Конкретный первый transport выбирается перед началом R5 по реально нужному integration path.

### Acceptance criteria

End-to-end сценарий должен демонстрировать:

1. создание task;
2. доставку worker;
3. выполнение bounded operation;
4. возврат result/evidence;
5. verified completion;
6. transport timeout/disconnect, который не превращается автоматически в failure;
7. reconciliation unknown outcome.

Transport-specific metadata не должно становиться каноническим task state.

## 9. Этап R6 — Watchdog, deferred work и escalation

### Цель

Реализовать отдельную ветку управления проблемами процесса и долгоживущими задачами.

### Результаты

- deferred task scheduling/queue semantics;
- watchdog observation без скрытой mutation основной задачи;
- сохранение прогресса основной ветки;
- explicit escalation routes:
  - другой worker;
  - другая/более сильная model через provider boundary;
  - пользователь;
- stop/escalation conditions;
- bounded retry/polling;
- budgets/limits там, где они нужны для предотвращения бесконечной работы.

### Acceptance criteria

Проблема диагностики или управления может быть выделена, отложена или эскалирована без потери identity/state/evidence основной задачи.

## 10. Этап R7 — Parallelism и hardening

### Цель

Проверить систему как многопользовательский/многопоточный control plane, а не последовательный скрипт.

### Проверки и результаты

- несколько параллельных tasks/workers;
- race tests на ownership и state transitions;
- duplicate delivery;
- stale worker/lease;
- backpressure и bounded queues;
- retention/GC под нагрузкой;
- security/redaction regression suite;
- protocol/provider/transport contract tests;
- Windows/Linux smoke/integration tests.

## 11. Этап R8 — Operational readiness

### Цель

Добавлять эксплуатационную инфраструктуру только после появления исполняемого runtime.

### Возможные результаты

- CI для реально существующих проверок;
- packaging/install/update strategy;
- observability и диагностические события;
- compatibility matrix;
- upgrade/rollback procedure для persisted state;
- operator/developer documentation;
- минимальный release process.

CI, packaging и deployment topology не создаются заранее ради самого наличия инфраструктуры.

## 12. Граница первого MVP

Первый полезный MVP считается достигнутым, когда завершены R1–R5 и система умеет:

- принять task через versioned control contract;
- сохранить устойчивую task/operation identity;
- назначить работу reference worker;
- наблюдать state transitions;
- получить evidence;
- подтвердить actual state;
- корректно пережить timeout/disconnect как unknown outcome;
- восстановиться после restart;
- провести хотя бы один реальный end-to-end transport path;
- выполнять это без привязки core к конкретной модели или transport.

Watchdog/deferred escalation (R6) является следующим обязательным слоем продукта, но не должен блокировать проверку базового control loop.

## 13. Decision gates

### Gate A — implementation stack

**Статус: выполнено 2026-09-15.** ADR-0001 выбирает Go 1.27.x, pure-Go и stdlib-first для reference core R2.

Открывается после R1. Нужен отдельный design decision с аргументами по Windows/Linux, tests, packaging и contract modeling.

### Gate B — storage

**Статус: выполнено 2026-09-15.** ADR-0002 выбирает SQLite через pure-Go `modernc.org/sqlite` для durable R3 state, с WAL, `synchronous=FULL`, revision CAS, persisted conflict reservations, migrations и bounded retention.

Открывается после минимального R2. Storage выбирается из фактических требований к persisted state, concurrency, migration и retention.

### Gate C — first transport

Открывается перед R5. Выбор определяется первым реальным E2E сценарием; transport contract остаётся внешним к core.

### Gate D — provider/model routing

Открывается после worker/provider contracts R4. Сначала проверяется adapter boundary, затем — policy выбора модели, стоимость и escalation.

## 14. Следующая bounded task

R1, Gate A, R2 и Gate B завершены. Exact-toolchain validation R2 на Go 1.27.1 также выполнена.

Следующая задача — **R3: durable state, recovery и retention**:

> Реализовать SQLite persistence boundary по ADR-0002: schema/migrations, atomic revision CAS, durable tasks/operations/attempts/messages/evidence/verifications, persisted mutation-scope reservations, restart recovery и bounded retention/GC. In-memory Store R2 не заменять transport-specific или SQL-specific логикой: persistence должен реализовать уже принятые core contracts. Не добавлять daemon topology, concrete transport или provider SDK.
