---
document_type: project_baseline
version: 1.0
status: active
updated_at: 2026-08-24
---

# agent_control_plane: baseline проекта

## 1. Назначение

`agent_control_plane` — переиспользуемая инфраструктура orchestration для координации ChatGPT Web, локальных и удалённых workers/agents, разных моделей и долгоживущих или отложенных задач.

Baseline фиксирует только устойчивые границы и инварианты, необходимые до составления roadmap. Он не задаёт этапы реализации, не выбирает конкретный runtime stack и не превращает прежний brainstorm в архитектуру.

## 2. Scope

К общему scope относятся механизмы, которые должны быть независимы от конкретной прикладной задачи, worker/model и transport:

- устойчивая identity задач и изменяющих операций;
- наблюдаемый state и correlation между запросами, workers, transports и результатами;
- bounded execution с явными условиями завершения, остановки и эскалации;
- evidence/receipts, позволяющие отличать заявленный результат от фактически подтверждённого состояния;
- watchdog/deferred handling ортогональных проблем рабочего процесса;
- взаимодействие Web ↔ agent на уровне orchestration;
- явная маршрутизация к workers/models/providers и к пользователю как escalation target;
- поддержка параллельных workers/requests без предположения о единственной последовательной команде;
- безопасная обработка timeout/disconnect и других unknown outcomes;
- ограниченное по времени/объёму хранение queue/log/replay/evidence и временных данных;
- версионируемые общие protocol/state contracts и совместимость их изменений, когда эти contracts появятся.

## 3. Out of scope и соседние проекты

`agent_control_plane` не поглощает обязанности соседних проектов без отдельного принятого решения.

- `opencode_setup` владеет bootstrap/deploy/reconciliation рабочего окружения.
- `ssh_relay` владеет transport удалённых команд, jobs и transfer. Его upstream transport contract не переносится в control plane.
- `agent-safe` владеет safety/recovery-подходом для изменяющих действий. Control plane может использовать такой подход, но не дублирует его ad hoc реализациями.
- `github-connector-knowledge` владеет общим регламентом и knowledge base по GitHub Connector/API и межпроектному GitHub workflow.
- Правила конкретных прикладных репозиториев остаются в этих репозиториях, пока не доказана необходимость общего механизма.

До отдельного решения также не входят в baseline: конкретная database, daemon/server/client, UI, provider SDK, transport implementation, language/framework, package manager и CI matrix.

## 4. Web-first как текущая рабочая модель

Текущая организация разработки — Web-first:

- ChatGPT Web выполняет архитектурное проектирование, сложный анализ, декомпозицию и review;
- локальный или удалённый worker получает ограниченную исполнимую задачу с ожидаемым состоянием, проверками и stop/escalation conditions.

Это рабочая модель разработки, а не продуктовая зависимость от ChatGPT Web. Архитектура control plane не должна без необходимости привязываться к одному UI, provider или модели.

## 5. Control protocol и transport

Control protocol и transport являются разными слоями.

Control protocol должен определять логическую identity, correlation, state, команды/события и evidence независимо от способа доставки. Transport отвечает за доставку в пределах собственного контракта и не должен автоматически становиться владельцем workflow state.

HTTPS relay с GET/PUT, VPS relay, P2P/NAT traversal, локальный IPC и другие варианты из прошлых обсуждений остаются кандидатами transport. Ни один из них этим baseline не выбран.

## 6. Task identity, state и evidence

Каждая задача и каждая значимая изменяющая операция должны иметь устойчивую identity, пригодную для correlation между параллельными запросами и повторными наблюдениями.

Состояние должно быть наблюдаемым и отличать как минимум намерение, запуск/принятие работы, промежуточное состояние и подтверждённый результат там, где соответствующие состояния будут определены будущим protocol/state design.

Утверждение о завершении не должно основываться только на сообщении worker или transport. Для значимых результатов требуется evidence, достаточное для проверки actual state на соответствующем слое.

Конкретная state machine и формат receipts/evidence ещё не выбраны.

## 7. Unknown outcome и безопасный retry

Timeout, disconnect, потеря transport или отсутствие ответа не доказывают failure изменяющей операции. Такое состояние считается unknown outcome, пока actual state не проверен.

Перед повтором mutation необходимо определить:

- фактическое состояние target;
- idempotency операции;
- ownership/correlation предыдущей попытки;
- риск дублирования или конфликтующего результата.

Автоматический повтор mutation после unknown outcome без этой проверки недопустим.

## 8. Параллельность, correlation и ownership

Несколько вкладок Web, workers, models и requests могут работать одновременно. Общая модель не должна полагаться на единственный глобальный последовательный поток команд.

Correlation и ownership должны позволять различать независимые задачи, вложенные операции, повторные попытки и результаты разных workers. Конкретный идентификаторный формат и concurrency model будут спроектированы позже.

## 9. Основная задача и ортогональная проблема процесса

Проблема управления/диагностики не обязана блокировать основную задачу и не должна скрыто менять её scope.

Архитектура должна допускать отделение проблемной ветки: её можно остановить или сохранить, передать watchdog/deferred processing либо эскалировать отдельно. Конкретная queue/watchdog architecture пока не принята.

## 10. Escalation routes

Эскалация является явным маршрутом control plane, а не скрытым fallback.

Допустимые классы targets:

- другой worker;
- другая/более сильная model через provider boundary;
- пользователь.

Порядок, критерии и стоимость маршрутизации пока не определены. Идея обратиться к более сильной модели до пользователя остаётся кандидатом для будущего проектирования, а не обязательным алгоритмом baseline.

## 11. Provider/worker abstraction

Claude, Qwen и другие модели/providers, а также различные локальные/удалённые workers должны подключаться через явные provider/worker boundaries.

Общая orchestration logic не должна зависеть от vendor-specific API без адаптера и проверяемого контракта. Конкретные adapters и поддерживаемые providers ещё не выбраны.

## 12. Bounded retention

Queue, logs, replay/evidence и временные данные не могут накапливаться бесконечно.

Для каждого сохраняемого класса данных будущая реализация должна определять bounded retention по времени, объёму или жизненному циклу owner/task, а также правила безопасного удаления. Конкретные бюджеты и storage mechanism baseline не задаёт.

## 13. Версионирование, compatibility и migration

Control protocol и сохраняемое (persisted) state должны иметь явную версию.

Несовместимое изменение protocol или persisted state не может полагаться на неявное совпадение версий. Для него должна быть определена compatibility/migration policy: поддерживаемые версии, правила чтения/записи и способ перехода или явного отказа.

Конкретные номера версий, форматы и migration mechanism будут определены вместе с соответствующими protocol/state designs.

## 14. Secrets и trust boundaries

Secrets, credentials, private keys, passwords, passphrases, authorization headers, cookies и relay/session tokens не должны попадать в обычные task payloads, logs, queues, receipts/evidence или Git.

Trust boundaries между Web, control plane, workers, providers и transports должны быть явными. Передача секретов, если она вообще понадобится, должна иметь отдельный минимально привилегированный механизм, не маскирующийся под обычный task payload.

## 15. Локальные платформы

Windows и Linux считаются first-class платформами локального runtime, пока scope явно не сужен отдельным решением.

Это не означает, что уже выбран язык, runtime, packaging или способ установки.

## 16. Что пока не принято

Следующие темы сознательно оставлены для roadmap и последующего design:

- окончательная protocol architecture и state machine;
- конкретная queue/storage architecture и database;
- daemon/server/client topology;
- конкретные transports и их приоритеты;
- model router и escalation policy;
- provider adapters и список поддерживаемых моделей;
- remote user interaction UI/channel;
- retention budgets;
- implementation language/framework/package manager;
- CI/test matrix после появления исполняемого runtime.

Идеи из прошлых обсуждений могут использоваться как входные данные для этих решений, но не считаются requirements, пока явно не приняты и не зафиксированы в документе-владельце.
