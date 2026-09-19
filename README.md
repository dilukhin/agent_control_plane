# agent_control_plane

`agent_control_plane` — верхнеуровневый control plane для координации ChatGPT Web, локальных и удалённых workers/agents, разных моделей и долгоживущих или отложенных задач.

## Текущий статус

Foundation, R1 и Gate A завершены. Минимальный reference core R2 реализован на pure Go: типизированные protocol/state/evidence contracts, in-memory reference store, revision/CAS, attempt/lease ownership, conflict scopes, duplicate-message handling и unknown-outcome/reconciliation semantics.

R2 проверен на точном target toolchain Go 1.27.1: `go test ./...`, `go vet ./...`, `go test -race ./...`, pure-Go cross-build для Windows/Linux и `gofmt` прошли. Validation record: [`docs/validation/r2-go-1.27.1.md`](docs/validation/r2-go-1.27.1.md).

Gate B завершён: для R3 выбран SQLite через pure-Go `modernc.org/sqlite`; R3.1 реализует локальную transactional DB с WAL/FULL, revision CAS, persisted conflict reservations и schema migrations. Общий core работает через persistence boundary с memory/SQLite; internal API и правила хранения описаны в [`docs/persistence.md`](docs/persistence.md). Recovery (R3.2) и bounded retention (R3.3) ещё предстоят; #10 остаётся открытой. Daemon/client/server topology, transport implementation и model router пока не выбраны.

## Ответственность проекта

В scope `agent_control_plane` входят общие механизмы orchestration:

- устойчивая identity задач и операций, их state, correlation и evidence;
- bounded execution и наблюдаемое завершение операций;
- watchdog/deferred escalation без смешивания основной задачи с проблемой управления процессом;
- взаимодействие Web ↔ agent на уровне control plane;
- явные маршруты эскалации к worker/model/user;
- provider/worker abstraction;
- координация параллельных workers/requests;
- безопасная работа с unknown outcome после timeout/disconnect;
- bounded retention очередей, журналов, replay/evidence и временных данных.

Границы ответственности:

- `opencode_setup` — bootstrap/deploy/reconciliation рабочего окружения;
- `ssh_relay` — transport удалённых команд, jobs и transfer; его transport-контракт остаётся upstream;
- `agent-safe` — safety/recovery-подход для изменяющих действий;
- `github-connector-knowledge` — общий регламент и knowledge base по GitHub Connector/API;
- project-specific правила остаются в соответствующих прикладных проектах, пока не доказана необходимость общего механизма.

## Текущая рабочая модель

Разработка ведётся Web-first: ChatGPT Web выполняет архитектурное проектирование, сложный анализ, декомпозицию и review, а локальные/удалённые агенты получают ограниченные исполнимые задачи. Это текущая модель разработки, а не обязательная привязка продукта к одному UI, provider или модели.

Control protocol должен быть отделён от transport. HTTPS relay, VPS, P2P/NAT traversal, локальный IPC и другие каналы могут рассматриваться как transports одного логического протокола, но конкретная transport architecture пока не выбрана.

## Нормативные документы

Канонический baseline проекта: [`docs/project_baseline.md`](docs/project_baseline.md).

Последовательность проектирования и реализации: [`docs/roadmap.md`](docs/roadmap.md).

Принятый порядок текущих работ и задачи: [`docs/work_plan_ru.md`](docs/work_plan_ru.md).

Принятые R1-контракты:

- [`docs/control_protocol.md`](docs/control_protocol.md) — control protocol v1;
- [`docs/state_model.md`](docs/state_model.md) — canonical state machine;
- [`docs/evidence_model.md`](docs/evidence_model.md) — evidence и actual-state verification;
- [`docs/security_model.md`](docs/security_model.md) — trust boundaries и security invariants;
- [`docs/decisions/0001-implementation-stack.md`](docs/decisions/0001-implementation-stack.md) — Gate A: Go 1.27.x для R2;
- [`docs/decisions/0002-durable-storage.md`](docs/decisions/0002-durable-storage.md) — Gate B: SQLite/modernc для R3;
- [`docs/development_environment.md`](docs/development_environment.md) — toolchain/environment validation rules.

Baseline фиксирует устойчивые инварианты и границы. Roadmap задаёт этапы и decision gates. R1-документы являются владельцами принятых contract-level решений; отложенные в них темы не считаются выбранной реализацией.

## Разработка

Содержательные изменения выполняются через отдельную task-ветку и pull request. Не добавлять implementation, dependency, CI или infrastructure «на будущее» без конкретной проверяемой потребности.

В репозитории есть Go unit/regression tests для protocol, state, evidence и core. Исторические результаты R2 приведены выше; они не заменяют проверки нового PR. Постоянный [Go CI](.github/workflows/go-ci.yml) запускает tests/vet/pure-Go build непосредственно на Windows/Linux и race/gofmt на Linux; требования описаны в [development_environment.md](docs/development_environment.md). Успешным CI считается фактический run/jobs для проверяемого commit, а не наличие workflow-файла.
