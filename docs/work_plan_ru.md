---
document_type: execution_plan
status: accepted
updated_at: 2026-09-19
---

# План работ agent_control_plane

## Основание и исходное состояние

План принят пользователем 2026-09-19 после инвентаризации main@ba800e8a1b1e25379f908ddce5b653017925a45d. PR #1–#7 слиты. R1/R2 и Gate A/B завершены; реализация содержит in-memory core. Выбор SQLite не означает готовность R3. Исторические tests/vet/race и cross-build R2 не доказывают native Windows tests или проверки будущего SQLite.

Архитектурные владельцы: [baseline](project_baseline.md), [roadmap](roadmap.md), R1 contracts и ADR-0001/0002. Этот документ задаёт порядок исполнения; новые transport/provider решения требуют своих gates.

## Последовательность

| Порядок | Задача | Результат и условие перехода |
| --- | --- | --- |
| 0 | План и актуализация документов | Согласованный порядок, исправленные сведения о текущем состоянии и связанные issues |
| 1 | [#9 — CI Windows/Linux](https://github.com/dilukhin/agent_control_plane/issues/9) | Native tests/vet и pure-Go build на обеих ОС; Linux race/gofmt. Реальный успешный run на актуальном head |
| 2 | [#10 — R3](https://github.com/dilukhin/agent_control_plane/issues/10) | Три PR: транзакционное хранилище → recovery → retention/GC. Закрыть задачу только после всех частей |
| 3 | [#11 — R4](https://github.com/dilukhin/agent_control_plane/issues/11) | Worker/provider contracts и замена двух тестовых реализаций без изменения orchestration |
| 4 | [#12 — R5](https://github.com/dilukhin/agent_control_plane/issues/12) | Gate C и один реальный transport; успешный цикл, timeout/disconnect и reconciliation. Первый MVP R1–R5 |
| 5 | [#8 — R6 и отложенный разбор ошибок](https://github.com/dilukhin/agent_control_plane/issues/8) | Ограниченная очередь, review batches, бюджеты, эскалация и evidence; prerequisite — R3–R5 и готовый контракт ScopedKB |
| 6 | R7 | Комплексные проверки параллельной работы, backpressure, stale workers, retention под нагрузкой и security regression |
| 7 | R8 | Установка, обновление, диагностика, совместимость состояния и выпуск |

R7/R8 остаются этапами roadmap. Их конкретные issues создаются при уточнении исполняемого объёма, без преждевременного выбора deployment topology. Проверки конкурентности и безопасности обязательны в каждом затронутом слое уже сейчас и не откладываются до R7.

## R3: три проверяемых изменения

### R3.1 — persistence и транзакции

- Persistence boundary сохраняет принятые core contracts; SQL не владеет orchestration logic.
- SQLite/modernc: закреплённая версия зависимости, схема и миграции, WAL/FULL, connection-scoped foreign keys и конечный busy timeout по ADR-0002.
- Сохраняются tasks, operations, attempts/leases, messages, evidence, verifications и conflict reservations.
- State/attempt/reservation/evidence изменяются атомарно; revision CAS и ограничения БД предотвращают конфликтующие записи.
- Приёмка: общий контрактный набор для in-memory/SQLite, rollback без частичного состояния, dedup/collision после reopen, migrations и отказ при newer schema.

### R3.2 — восстановление

- Restart различает verified completion, known failure, active/recoverable work и unknown outcome.
- Lease expiry/restart не освобождает reservation и не разрешает повтор изменяющего действия автоматически.
- Приёмка: crash/restart, stale owner, конкурентные процессы, ограниченное ожидание, safe verification до снятия reservation/новой попытки.

### R3.3 — сроки хранения и очистка

- Явные бюджеты для каждого класса данных; конечные batch sizes и обслуживание БД вне критического пути.
- Unresolved unknown, active reservations, необходимые evidence и dedup records защищены от очистки по одному возрасту.
- Приёмка: отсутствие orphan evidence, проверка ограниченного объёма очистки и native Windows/Linux open/restart/migration/retention.

## Параллельная работа

Основную последовательность CI → R3 → R4 → R5 → R6 выполняем последовательно, начиная каждый этап от подтверждённого main.

Параллельно R3 допустима только часть A #8: design mapping ReviewBatch на Task/Operation/Attempt/Evidence, согласование pending/context/result interfaces со [ScopedKB #15](https://github.com/dilukhin/scopedkb/issues/15), ограничения размера/времени/стоимости и синтетические fixtures. Это не разрешает создавать второй scheduler или внедрять незавершённый provider/transport.

Часть B #8 начинается после R3–R5 и доказанного контракта ScopedKB. Неизвестный результат изменяющего действия требует немедленного reconciliation; анализ причины можно отложить отдельно. Предложение исправления не даёт автоматического права его применить.

## Проверки и завершение

- Каждое связное изменение — отдельная ветка/PR; R3 разбит на три PR.
- Перед публикацией — проверки изменённого слоя; после публикации — read-back файлов/PR и проверка Actions run/jobs на фактическом head.
- После review и успешных обязательных проверок — слияние с контролем head, read-back main и обновление issue.
- Отсутствие проверки, skipped/cancelled job или старая проверка другого commit не считается pass.
- Новая архитектурная развилка, конфликт ownership или неожиданное фактическое состояние останавливают mutation chain; сначала диагностика и явное решение.
- Не понижать Go baseline ради среды и не считать ephemeral toolchain сохранённым между диалогами.

## Сопутствующие документы

В github-connector-knowledge профиль проекта ещё содержит историческое ограничение «только foundation, tests/CI отсутствуют». После появления CI актуализировать исходный профиль и сформировать runtime bundle штатным генератором через отдельный PR; generated dist не редактировать вручную. Это долг актуализации профиля, а не новый инцидент GitHub Connector.
