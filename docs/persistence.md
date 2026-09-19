# Persistence R3.1

Реализует [ADR-0002](decisions/0002-durable-storage.md). Orchestration decisions остаются в `internal/core`; `internal/persistence` задаёт `Reader`, `Tx`, `Repository`, а `memory` и `sqlite` реализуют транзакционное хранение. Закреплён `modernc.org/sqlite v1.58.0`; transitive dependencies фиксируются `go.mod`/`go.sum`. Protocol version остаётся `1.0`.

## Внутренний API и атомарность

`core.NewStore()` создаёт reference memory store. Durable store создаётся через `sqlite.Open(ctx, localPath, options)` и `core.NewWithRepository(repository)`; владелец вызывает `Store.Close()` после завершения работы.

Изменение внутреннего API R2 явное: методы `Store` принимают `context.Context`; `GetTask`/`GetOperation` возвращают `(value, error)` вместо `(value, bool)`, `EvidenceForOperation` также возвращает error. `ErrNotFound` отличает отсутствие записи от ошибки БД. Добавлены чтение attempt/verification и `ProcessMessage`. Внешних adapters пока нет; все существующие callers в репозитории обновлены.

Каждый изменяющий метод выполняется в одной transaction. Для связанного приёма сообщения, evidence и state decision следует использовать:

```go
_, err := store.ProcessMessage(ctx, envelope, func(tx *core.Transaction) error {
    if err := tx.RegisterEvidence(record); err != nil {
        return err
    }
    _, err := tx.ApplyVerification(operationID, expectedRevision, verification, now)
    return err
})
```

Duplicate message пропускает callback. Callback error, panic или cancellation откатывают все записи. Ошибка любого метода `core.Transaction` помечает всю transaction ошибочной, даже если callback случайно её проигнорировал. Возвращённые из callback значения до успешного commit предварительны. Callback синхронный и ограниченный; нельзя сохранять `Transaction`, вызывать вложенный repository/Store или выполнять внешние side effects. Context ограничивает storage operations и запрещает commit после cancellation, но не прерывает произвольный Go-код callback.

`RegisterMessage` отдельно регистрирует только dedup metadata; это не доказательство исполнения команды. Его нельзя использовать как отдельный предварительный commit перед связанным state change. Ни repository, ни core автоматически не повторяют callbacks. Commit error/timeout требует чтения actual state; это не свидетельство отсутствия commit.

`MarkReady` разрешён только для первого перехода planned → ready. Executing/verifying → ready требует сохранённого retry-safe verification через `MarkNotStarted`/`ApplyVerification`. Revision и lease generation ограничены положительным signed 64-bit диапазоном SQLite и не переполняются. Попытка `retry_of` должна принадлежать той же operation. Verification IDs уникальны, ссылки на evidence проверяются; evidence list не содержит дубликатов.

## Физическое хранение

| Данные | Представление и защита |
| --- | --- |
| Tasks, operations | STRICT typed columns; operation revision обновляется через `WHERE id=? AND revision=?`, `RowsAffected == 1` |
| Attempts и leases | Immutable identity/owner/generation; уникальные attempt ID, lease ID и generation внутри operation |
| Scopes/reservations | Отдельные таблицы; scope — глобальный PK reservation; ссылки на operation scope и attempt/lease/generation |
| Evidence/verifications | Typed records и ordered junction table с составными foreign keys; verification сохраняется вместе с решением |
| Messages | ID, correlation identities, issued time и versioned digest; payload не сохраняется |
| Migration history | Application ID `0x41435031`, `user_version`, последовательная история с SHA-256 каждого SQL migration |

Descriptor immutable; порядок conflict scopes сохраняется. Evidence listing упорядочен по ID. Время сохраняется UTC RFC3339Nano, monotonic clock component не сохраняется. Empty optional attempt references представлены SQL NULL. Ownership проверяется вместе с canonical operation state; reservations не истекают автоматически и сохраняются для `unknown_outcome`.

Digest v1: проверенный envelope с UTC `issued_at`/`deadline_at` сериализуется стандартным Go `encoding/json`, затем вычисляется `v1:` + SHA-256 hex. Ключи maps сериализуются детерминированно; одинаковые JSON-числа разных Go numeric types эквивалентны. Это локальное versioned правило, не заявление о RFC 8785. Иное содержимое при том же message ID даёт collision. При смене алгоритма требуется явная migration/compatibility policy.

## Открытие и настройки

Только локальный regular file на том же host; active DB на NFS/SMB не поддерживается. Caller заранее создаёт private parent directory: POSIX permissions или Windows ACL. Новый файл создаётся с mode `0600` на POSIX; ACL Windows наследуется от директории. Существующие permissions не переписываются. Последний path component не может быть symlink; доверенная parent directory обязательна. DB/WAL/SHM являются одной operational unit — не копировать один active DB-файл как backup.

Пул ограничен четырьмя соединениями. WAL, FULL synchronous, foreign keys и finite busy timeout применяются/проверяются; DSN применяет connection-scoped настройки на каждом новом соединении. Busy timeout по умолчанию 5 s, transaction budget 10 s, допустимый диапазон каждого параметра 1 ms–30 s. При заимствовании соединения lock wait дополнительно ограничивается оставшимся context budget. Writes используют `BEGIN IMMEDIATE`, reads — snapshot transaction. Rollback использует отдельный конечный context; соединение исключается из пула, если rollback не подтверждён.

Новая БД получает `auto_vacuum=INCREMENTAL` до первой schema transaction. Включение режима не означает реализацию GC: вызовы bounded vacuum и retention policy относятся к R3.3. На открытии выполняются `quick_check` и `foreign_key_check`.

## Версии и миграции

Первая persisted schema — v1; текущая v2 добавляет revocations для R3.2. R2 хранил только память, поэтому единственная допустимая предыдущая fixture — пустая БД v0. Импорт произвольной SQLite-БД не выполняется. Embedded SQL migrations выполняются последовательно в transaction вместе с историей и номером версии; при ошибке DDL/data/history/version откатываются. Нет down migrations, удаления или автоматического пересоздания incompatible DB.

Unsupported newer version, иной application ID, непоследовательная/изменённая история отклоняются. Schema version/history повторно проверяются внутри каждого read/write transaction, поэтому уже открытый старый handle не пишет после upgrade другим process. История migration проверяет совместимость, но не является защитой от злонамеренного изменения БД владельцем файлов. При ошибке открытия файл сохраняется для диагностики.

Для следующей schema migration нужен новый numbered SQL-файл и previous-version fixture; опубликованный SQL v1 нельзя редактировать. Перед upgrade следует остановить writers и сделать согласованный backup. Не запускать старый binary на обновлённом файле; при необходимости возврата использовать совместимый binary и согласованный backup по отдельной процедуре, без автоматического destructive recovery.

## Граница готовности

R3.1 обеспечивает persistence и атомарность. Open/reopen не запускает scheduler, не классифицирует работу и не повторяет execution. Recovery inventory и отзыв полномочий реализованы в [R3.2](recovery.md); bounded retention/GC — к R3.3. До их завершения #10 открыт, долговременная production эксплуатация R3 не заявляется. Тесты и границы доказательств: [validation record](validation/r3.1-sqlite.md).
