---
document_type: security_model
document_version: 0.1
status: proposed
updated_at: 2026-09-15
---

# Security model v1

## Назначение

Документ фиксирует trust boundaries и обязательные ограничения `agent_control_plane` до выбора runtime stack.

Главный принцип: orchestration data, execution authority и чувствительные данные — разные классы и не смешиваются в обычном task payload.

## Защищаемые активы

К защищаемым активам относятся:

- canonical task/operation state;
- authorization decisions;
- execution ownership/leases;
- evidence/verification history;
- user decisions;
- target systems, над которыми выполняются mutations;
- private project data;
- целостность protocol/state versions.

## Trust boundaries

Минимальные logical zones:

1. **User/operator boundary** — человек или UI, формирующий цели и approvals.
2. **Control core** — владелец canonical state и state-machine decisions.
3. **Persistence boundary** — state/evidence storage.
4. **Worker boundary** — локальный/удалённый исполнитель.
5. **Provider/model boundary** — внешний или локальный model provider.
6. **Transport boundary** — канал доставки.
7. **Target boundary** — фактический ресурс, над которым выполняется operation.
8. **Privileged secret-resolution boundary** — отдельный механизм доступа к закрытым значениям, если он понадобится.

Ни transport, ни provider, ни worker не считаются владельцами canonical state по умолчанию.

## Trust assumptions

### Control core

Доверяется для:

- применения state transitions;
- выдачи execution leases;
- canonical correlation;
- выбора verification path по policy.

### Worker

Worker — ограниченно доверенный executor:

- выполняет только bounded operation;
- сообщает claims/evidence;
- не объявляет verified completion;
- не расширяет scope скрыто;
- неожиданную архитектурную развилку возвращает как escalation/evidence.

### Provider/model

Model output считается недоверенным результатом до применения control policy.

Provider не получает автоматически полный project context или чувствительные данные.

### Transport

Transport отвечает за доставку в пределах своего контракта.

Delivery status не равен workflow state. Потеря transport не разрешает повтор mutation.

### Target

Target state является источником actual-state verification на соответствующем слое через явно авторизованный verifier/adapter.

## Чувствительные данные

Обычные:

- task payload;
- command/event envelope;
- queue;
- log;
- replay;
- receipt;
- evidence;
- Git

не должны содержать закрытые значения доступа.

Если operation требует privileged material, control message может содержать только opaque `secret_ref`, который:

- не раскрывает значение;
- разрешается только в authorized secret-resolution boundary;
- имеет минимально необходимый scope;
- не возвращается worker report/evidence;
- не логируется вместе с resolved value.

Конкретный secret store/mechanism не выбирается на R1.

## Actor identity и authentication

`actor_id` — identity, а не credential.

Authentication выполняется boundary-specific механизмом. Adapter передаёт core подтверждённую actor identity/capabilities.

Core не доверяет self-declared role из неподтверждённого payload.

## Authorization

Authorization принимается до выдачи execution lease/command.

Минимальные измерения:

- actor;
- operation class;
- target scope;
- mutation/read-only;
- required capability;
- optional approval requirement;
- lease ownership.

Worker не получает широкую authority только потому, что отдельный task требует privileged step.

Принцип least privilege обязателен.

## Capability boundaries

Worker/provider adapter объявляет capabilities отдельно от model prompt.

Нужно различать:

- capability available;
- capability authorized for task;
- capability actually invoked;
- result verified.

Наличие возможности не равно разрешению её использовать.

## Task/content как недоверенные данные

Task text, repository files, web pages, model output и worker-generated text могут содержать инструкции, конфликтующие с control policy.

Такие данные:

- не расширяют capabilities;
- не меняют authorization;
- не подменяют protocol/system policy;
- не разрешают disclosure закрытых значений;
- не инициируют hidden transport/provider fallback.

Исполняемое действие возникает из explicit bounded operation.

## Replay и duplicate protection

Базовые механизмы:

- globally unique `message_id`;
- stable `operation_id`;
- per-attempt `attempt_id`;
- lease generation;
- canonical state revision;
- deduplication до side effect, где это возможно;
- target idempotency только при подтверждённой поддержке.

Повтор старой команды от stale lease не становится новой authorized mutation.

## Unknown outcome как security property

Unknown outcome предотвращает двойную mutation после network/worker failure.

После timeout/disconnect:

- automatic retry запрещён;
- выполняется read-only reconciliation;
- новая mutation требует evidence retry safety;
- stale worker result не игнорируется, если может означать side effect.

## Transport security

Control protocol не выбирает TLS/SSH/IPC, но production adapter должен определить:

- peer authentication;
- confidentiality/integrity;
- replay considerations;
- endpoint identity;
- timeout semantics;
- bounded message size;
- failure mapping в control protocol.

Transport security не заменяет canonical authorization.

## Provider privacy boundary

Перед отправкой данных provider adapter минимизирует context до требуемого.

Должна быть возможность:

- исключить закрытые фрагменты;
- редактировать private data;
- выбирать provider по data-handling constraints;
- фиксировать bounded context class, не копируя private payload в обычные logs.

Конкретная provider policy проектируется на R4.

## Logging

Logs по умолчанию содержат:

- ids;
- event names;
- state transitions;
- bounded diagnostics;
- redacted error summaries.

Raw task/model payloads не логируются без явной необходимости и отдельной policy.

## Evidence security

Evidence следует `docs/evidence_model.md`.

Дополнительно:

- provenance не принимается как доверенная self-declared строка;
- artifact refs не содержат bearer-style access data;
- large raw logs проходят redaction;
- correction оформляется новым record;
- sensitive evidence получает bounded access/retention policy.

## Persistence

Persisted state должен:

- различать schema/protocol version;
- поддерживать identity/revision integrity;
- не хранить закрытые значения как обычные records;
- иметь bounded retention;
- безопасно переживать restart.

Конкретный механизм защиты storage выбирается вместе с deployment threat model.

## High-risk mutations

Для irreversible/high-blast-radius actions control policy должна поддерживать:

- pre-state verification;
- explicit approval при необходимости;
- narrower capability;
- read-only discovery/dry-run, если доступен;
- post-state verification;
- stop on unexpected state.

Это согласуется с `agent-safe`, но control plane не дублирует safety logic ad hoc.

## Orthogonal issue isolation

Watchdog/diagnostic issue:

- не наследует автоматически authority parent task;
- имеет собственную identity;
- использует отдельные bounded operations;
- не может скрыто исправлять основной target;
- может явно эскалировать решение.

## Failure handling

При security-relevant unexpected state mutation chain прекращается.

Далее допустимы:

- read-only diagnostics;
- evidence collection;
- explicit escalation;
- controlled recovery с новой authorization.

Скрытые delete/reset/force действия не считаются универсальным rollback.

## Threat scenarios

### Stale worker присылает поздний success

Mitigation: lease generation + state revision; поздний report становится evidence, не authoritative transition.

### Transport дублирует mutation command

Mitigation: message dedup + operation/attempt identity + target idempotency where supported.

### Недоверенный контент пытается расширить scope

Mitigation: payload не меняет authorization/capabilities; требуется explicit bounded operation.

### Timeout после mutation

Mitigation: unknown outcome + read-only reconciliation before retry.

### Worker расширяет scope

Mitigation: capability + target scope authorization; unexpected branch → escalation.

### Параллельные controllers конфликтуют

Mitigation: canonical revision compare-and-set и одна authoritative lease generation для serial mutation.

### Диагностический вывод содержит закрытые данные

Mitigation: minimization/redaction и regression tests на implementation stage.

## Cross-platform requirements

Windows и Linux first-class.

Security design не должен зависеть исключительно от:

- POSIX permissions;
- Unix-domain sockets;
- systemd;
- Windows-only ACL/credential mechanism.

Platform-specific protection реализуется adapter/runtime boundary с общим contract.

## Acceptance criteria R1

R1 достаточен для implementation design, если:

- trust boundaries определены;
- canonical state owner определён;
- actor identity отделена от authentication;
- authorization отделена от capability availability;
- duplicate/replay/unknown-outcome semantics определены;
- worker/provider/transport claims не равны verified result;
- high-risk mutation path допускает approval/read-back;
- platform assumptions не сужены до одной ОС.

## Отложенные решения

R1 не выбирает authentication provider, cryptographic protocol, PKI, secret store, OS credential backend, sandbox technology, network ACL, provider-specific privacy configuration и storage encryption implementation.
