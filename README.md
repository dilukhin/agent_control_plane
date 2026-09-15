# agent_control_plane

`agent_control_plane` — верхнеуровневый control plane для координации ChatGPT Web, локальных и удалённых workers/agents, разных моделей и долгоживущих или отложенных задач.

## Текущий статус

Foundation-этап завершён. Проект находится на стадии roadmap/design: последовательность дальнейшей разработки зафиксирована в [`docs/roadmap.md`](docs/roadmap.md).

Product implementation, выбранный язык/framework, runtime, protocol implementation, storage, daemon/client/server, transport implementation и model router пока отсутствуют. Их выбор выполняется только в соответствующих decision gates roadmap.

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

Baseline фиксирует устойчивые инварианты и границы. Roadmap задаёт этапы и decision gates, но не превращает ещё не принятые варианты реализации в архитектурные решения.

## Разработка

Содержательные изменения выполняются через отдельную task-ветку и pull request. Не добавлять implementation, dependency, CI или infrastructure «на будущее» без конкретной проверяемой потребности.

Пока в репозитории нет тестов и CI jobs, поэтому никакие такие проверки не считаются существующими.
