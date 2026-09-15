---
document_type: architecture_decision
decision_id: ADR-0002
status: accepted
date: 2026-09-15
owners:
  - agent_control_plane
---

# ADR-0002: durable storage для R3

## Решение

Для durable state R3 выбран **SQLite** через pure-Go драйвер **`modernc.org/sqlite`**.

Storage остаётся локальным embedded-компонентом control plane и не становится владельцем orchestration semantics: state machine, identity, retry/unknown-outcome rules и verification policy остаются в core.

Начальная R3 реализация должна использовать:

- `database/sql` из стандартной библиотеки;
- `modernc.org/sqlite` как единственный новый runtime dependency, необходимый для SQLite без cgo;
- один SQLite database file на один logical control-plane state store;
- локальную файловую систему того же host; network filesystem для active DB не поддерживается;
- `journal_mode=WAL`;
- `synchronous=FULL` для canonical state/evidence durability;
- `foreign_keys=ON` на каждой физической connection;
- конечный `busy_timeout` с configurable bounded default;
- `STRICT` tables для state-critical schema;
- `auto_vacuum=INCREMENTAL` при создании новой DB и bounded incremental vacuum как часть retention maintenance.

На дату решения актуальный `modernc.org/sqlite` v1.58.0 использует SQLite 3.53.4 и остаётся pure Go/no-cgo. Конкретная dependency version должна быть повторно проверена и зафиксирована при R3 implementation; молчаливое обновление dependency недопустимо.

## Почему storage нужен именно сейчас

R2 подтвердил реальные требования, которые нельзя надёжно моделировать простым файлом-снимком:

- atomic state transition вместе с attempt/lease/evidence/message metadata;
- revision compare-and-set (CAS) при параллельных controllers;
- durable message deduplication после restart;
- сохранение mutation conflict reservations через crash;
- migration/versioning persisted state;
- indexed retention/GC;
- восстановление `executing`, `verifying` и `unknown_outcome` без автоматического retry;
- Windows/Linux без обязательного внешнего DB server.

## Рассмотренные варианты

### SQLite + modernc.org/sqlite — выбран

Преимущества:

- SQLite даёт ACID/serializable transactions и атомарный commit даже при crash/power failure;
- несколько connections/processes на одном host могут читать базу, writes сериализуются самим SQLite;
- WAL позволяет readers работать параллельно с writer;
- SQL позволяет выражать revision CAS обычным conditional UPDATE и проверкой RowsAffected;
- constraints/foreign keys/unique indexes помогают физически закрепить identity и ownership invariants;
- schema migrations и retention queries естественны;
- один self-contained database file плюс WAL/SHM operationally проще внешнего DB server;
- `modernc.org/sqlite` предоставляет SQLite через `database/sql` без cgo и поддерживает Go deployment model R2.

Цена:

- это первый значимый third-party runtime dependency;
- dependency заметно тяжелее bbolt;
- SQLite допускает только одного writer одновременно;
- WAL требует все active processes на одном host и не должен использоваться поверх network filesystem;
- connection-scoped PRAGMA settings необходимо применять/проверять на каждой physical connection.

Для ожидаемого control-plane workload single-writer serialization является скорее полезной safety boundary, чем performance bottleneck. Если profiling позднее докажет обратное, решение можно пересмотреть.

### bbolt — отклонён для canonical store

Плюсы:

- pure Go;
- маленькая API surface;
- fully serializable ACID transactions;
- multiple readers + one writer;
- Windows/Linux;
- очень простая embedded deployment model.

Причина отказа: upstream bbolt держит file lock так, что несколько процессов не могут открыть одну DB одновременно. Это преждевременно фиксирует будущую topology как single-process owner. Кроме того, migration/query/retention/indexing и relational integrity пришлось бы реализовывать вручную поверх key/value layout.

bbolt остаётся возможным кандидатом для isolated caches или single-process auxiliary stores, но не canonical R3 state.

### BadgerDB — отклонён

Плюсы:

- pure Go;
- concurrent ACID transactions с SSI;
- TTL/versioned KV и высокая write throughput.

Причина отказа: LSM/value-log architecture и operational surface рассчитаны на существенно более тяжёлые workloads. Для небольшого control-plane state это повышает complexity/dependency cost, а schema migrations, relational constraints и audit queries всё равно остаются application-level задачей.

### Pebble — отклонён

Pebble — зрелый pure-Go LSM KV engine, но его собственная документация прямо относит transactions к отсутствующим RocksDB features. Для R3 нужна атомарная multi-record state transition semantics, поэтому batch/snapshot API недостаточны как canonical persistence contract.

### Внешний PostgreSQL/другой server DB — отложён

Server database дал бы сильную multi-process/multi-host concurrency, но сейчас это:

- новый обязательный service;
- installation/configuration/credential lifecycle;
- дополнительная failure domain;
- преждевременное topology решение до появления evidence, что embedded DB недостаточна.

Переход к server DB рассматривается только по измеримой необходимости.

## Transaction model

### Canonical mutation transaction

Каждый state-changing control-plane decision должен быть одной SQLite transaction, включающей все связанные durable изменения, например:

1. validate expected operation revision;
2. insert/update attempt/lease;
3. reserve/release conflict scopes;
4. register required evidence/verification/message metadata;
5. update canonical state and increment revision;
6. commit.

Partial commit этих частей запрещён.

### Revision CAS

State transition физически закрепляется условным update:

```sql
UPDATE operations
SET state = ?, revision = revision + 1, updated_at = ?
WHERE operation_id = ? AND revision = ?;
```

`RowsAffected == 1` означает успешный CAS. `0` означает revision conflict; caller перечитывает actual state и пересчитывает решение. Автоматический blind retry transaction не должен превращаться в semantic mutation retry.

### Mutation conflict reservations

Пересекающиеся mutation scopes должны иметь отдельную persisted reservation table с unique/primary-key constraint на scope key.

Reservation содержит как минимум:

- scope;
- operation_id;
- attempt_id;
- lease_id/generation;
- created/updated metadata.

Все scopes одного attempt резервируются в той же transaction, что переводит operation в `executing`.

Reservation не удаляется только по timeout/lease expiry. Для `unknown_outcome` она сохраняется до reconciliation, доказавшего safe release/retry.

Это обеспечивает mutual exclusion не только между goroutines одного процесса, но и между локальными control-plane processes, если такая topology появится.

## Persisted entities

R3 schema должна явно представлять как минимум:

- tasks;
- operations;
- operation conflict scopes;
- attempts/leases;
- active mutation scope reservations;
- messages/dedup digest;
- evidence;
- verifications;
- migration metadata.

State-critical identity/state/revision fields хранятся typed columns, а не только opaque JSON/blob.

Opaque/versioned payload допустим только для contract fields, которые не участвуют в storage-level invariants. Storage encoding не становится wire protocol.

## Message deduplication

`message_id` должен иметь UNIQUE/PRIMARY KEY constraint.

Для accepted message сохраняется deterministic content digest. Повтор того же `message_id`:

- с тем же digest → duplicate/replay, без повторного side effect;
- с другим digest → collision/integrity error.

Dedup metadata переживает restart и удаляется только по retention policy, согласованной с owner task lifecycle.

## Restart/recovery

Startup никогда не трактует restart как failure уже начатой mutation.

После открытия DB:

1. schema version проверяется/мигрируется;
2. integrity/required PRAGMA configuration проверяется;
3. non-terminal tasks/operations загружаются;
4. persisted `executing`/stale lease records оцениваются recovery policy;
5. если physical executor quiescence/result не доказаны, mutation идёт в `unknown_outcome`/reconciliation, а reservation сохраняется;
6. никакой mutation retry не создаётся автоматически только из-за restart.

Persisted reservations являются safety mechanism, а не мусором, который можно чистить по TTL.

## Migration policy

Миграции:

- monotonic forward migrations, встроенные в binary;
- применяются до запуска обычной orchestration работы;
- каждая migration выполняется transactionally;
- source schema version должна точно соответствовать поддерживаемой migration path;
- неизвестная более новая schema → fail closed/read-only diagnostics, не downgrade;
- migration fixtures обязаны покрывать supported previous versions.

Source of truth — явная `schema_migrations`/metadata table с version и checksum/identity migration. `PRAGMA user_version` может использоваться как дополнительный fast guard/read-back, но не заменяет migration history.

Автоматический destructive down-migration не поддерживается.

## SQLite configuration

### WAL

WAL выбран из-за reader/writer concurrency. Он допустим только на local filesystem одного host.

Database, `-wal` и `-shm` являются одной operational state unit. Нельзя делать live backup простым копированием только основного `.db` файла.

### synchronous=FULL

Canonical task/operation state влияет на решение, безопасно ли повторять mutation. Потеря уже подтверждённого commit после power loss может привести к опасному повтору.

Поэтому R3 использует `synchronous=FULL`, а не `NORMAL`: SQLite документирует, что WAL+NORMAL сохраняет consistency, но recent committed transaction может потеряться при power loss.

Ослабление durability возможно только отдельным design decision для данных, потеря которых доказанно безопасна; canonical state/evidence к таким данным не относятся.

### foreign_keys=ON

Foreign keys включаются и read-back проверяются для каждой physical connection. Нельзя полагаться на default SQLite, потому что foreign key enforcement historically disabled by default per connection.

### busy timeout

Lock contention не должен превращаться в бесконечное ожидание.

Каждая connection получает конечный busy timeout. Начальный рекомендуемый default для R3 — 5 seconds, configurable и bounded. После `SQLITE_BUSY` control plane перечитывает actual state/ownership; он не делает бесконечный polling и не интерпретирует busy как semantic failure target operation.

### STRICT tables

State-critical tables создаются `STRICT`, чтобы SQLite enforcing types помогал ловить schema/application mismatch. Это не заменяет Go validation и constraints.

### Incremental vacuum

Новые DB создаются с `auto_vacuum=INCREMENTAL`. Retention worker удаляет данные bounded batches и отдельно выполняет bounded `incremental_vacuum(N)`.

Полный `VACUUM` не используется на критическом execution path; он может быть отдельной maintenance operation с preconditions и достаточным свободным disk space.

## Retention/GC

R3 должен сделать retention явным для каждого сохраняемого класса.

Минимальная модель:

- immutable verification/evidence, требуемые terminal или unresolved unknown-outcome history, нельзя удалить раньше owner record;
- ephemeral diagnostics могут иметь короткий `expires_at`;
- message dedup record живёт не меньше replay window/owner task requirement;
- terminal task data удаляется только после policy-defined retention;
- unresolved `unknown_outcome` и active reservations не GC по возрасту;
- GC работает bounded batches и транзакциями, не unbounded full-table delete;
- FK/constraints должны предотвращать orphaning critical evidence.

Конкретные retention durations/budgets определяются в R3 implementation/config, а не этим ADR.

## Security и filesystem

- DB file содержит private operational state/evidence, но не ordinary secret values.
- Secret prohibition R1 остаётся действующим; SQLite не становится secret store.
- File/directory permissions должны быть platform-appropriate и минимально необходимыми.
- Active DB на NFS/SMB/другом network filesystem не поддерживается.
- Backup/export не должен включать секреты, отсутствующие в canonical records.
- Corrupt/incompatible DB приводит к fail-closed для mutations и read-only diagnostics/recovery path.

Encryption-at-rest не выбирается этим ADR; она зависит от deployment threat model и не оправдывает запись secrets в canonical storage.

## R3 acceptance implications

После Gate B R3 должен доказать тестами как минимум:

- transaction atomicity на state + attempt + reservation + evidence;
- revision CAS conflict;
- duplicate message across close/reopen;
- restart with `executing` mutation does not auto-retry;
- `unknown_outcome` and reservation survive restart;
- safe verification releases reservation and permits new attempt;
- migration from previous fixture;
- unsupported newer schema fails closed;
- retention does not delete evidence referenced by live/unknown state;
- concurrent goroutine/process write contention remains bounded;
- Windows/Linux open/restart/migration smoke.

## Revisit conditions

Пересмотреть SQLite следует только при evidence, что:

- required canonical write throughput/latency не укладывается в single-writer model;
- нужна active multi-host shared state;
- database size/retention pattern делает SQLite maintenance неприемлемым;
- modernc driver создаёт доказанную portability/reliability проблему;
- external operational requirements уже предполагают managed server DB.

До такого evidence переход на distributed/server storage считается преждевременным.

## Источники

Проверены 2026-09-15:

- SQLite transactional guarantees: https://www.sqlite.org/transactional.html
- SQLite isolation/concurrency: https://www.sqlite.org/isolation.html
- SQLite WAL: https://www.sqlite.org/wal.html
- SQLite PRAGMAs: https://www.sqlite.org/pragma.html
- SQLite foreign keys: https://www.sqlite.org/foreignkeys.html
- SQLite STRICT tables: https://www.sqlite.org/stricttables.html
- SQLite VACUUM: https://www.sqlite.org/lang_vacuum.html
- modernc.org/sqlite documentation: https://pkg.go.dev/modernc.org/sqlite
- modernc.org/sqlite changelog: https://gitlab.com/cznic/sqlite/-/blob/master/CHANGELOG.md
- bbolt README/documentation: https://github.com/etcd-io/bbolt
- BadgerDB README: https://github.com/dgraph-io/badger
- Pebble README: https://github.com/cockroachdb/pebble
