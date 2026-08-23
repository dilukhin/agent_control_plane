# agent_control_plane

`agent_control_plane` — верхнеуровневый control plane для координации ChatGPT Web, локальных и удалённых workers/agents, разных моделей и долгоживущих или отложенных задач.

## Текущий статус

Репозиторий находится на стадии foundation. Здесь ещё нет product implementation, roadmap, выбранного языка/framework, runtime, protocol implementation, storage, daemon/client/server или model router.

Следующий отдельный этап проектирования должен определить roadmap. Идеи из предыдущих обсуждений не считаются реализованными возможностями или автоматически принятыми требованиями.

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

## Нормативный baseline

Канонический baseline проекта: [`docs/project_baseline.md`](docs/project_baseline.md).

Он фиксирует только устойчивые инварианты и границы, необходимые до roadmap. Если утверждение отсутствует в baseline либо явно отмечено как кандидат/идея, оно не считается принятым архитектурным решением.

## Разработка

Содержательные изменения выполняются через отдельную task-ветку и pull request. Не добавлять implementation, dependency, CI или infrastructure «на будущее» без конкретной проверяемой потребности.

Пока в репозитории нет тестов и CI jobs, поэтому никакие такие проверки не считаются существующими.
