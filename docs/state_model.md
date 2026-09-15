---
document_type: state_model
document_version: 0.1
status: proposed
updated_at: 2026-09-15
---

# State model v1

## Назначение

Модель определяет canonical state для task, operation, attempt, issue и escalation. Transport status, worker claim и actual target state рассматриваются как разные факты.

Каждый сохраняемый объект имеет stable identity, монотонный `revision`, timestamps и ссылки на causal messages/evidence. Canonical state меняет только control plane state machine.

## Task state

Task states:

- `planned`;
- `active`;
- `blocked`;
- `awaiting_decision`;
- `completed`;
- `failed`;
- `cancelled`.

Terminal: `completed`, `failed`, `cancelled`.

Task не может стать `completed`, если required mutation остаётся в `unknown_outcome`. Timeout/disconnect сам по себе не делает task failed.

## Operation state

Operation states:

- `planned` — operation определена, но ещё не готова;
- `ready` — prerequisites выполнены, новый attempt разрешён;
- `executing` — существует active attempt/lease;
- `verifying` — требуется проверка actual state;
- `unknown_outcome` — side effect возможен, но результат недостаточно известен;
- `succeeded` — expected actual state подтверждён;
- `failed` — terminal failure подтверждён, retry больше не разрешён;
- `cancelled` — доказано безопасное прекращение без unresolved side effect.

Terminal: `succeeded`, `failed`, `cancelled`.

`unknown_outcome` non-terminal, но блокирует semantic retry.

## Operation transitions

| From | To | Условие |
|---|---|---|
| planned | ready | prerequisites удовлетворены |
| ready | executing | создан attempt и active lease |
| executing | verifying | получен result/report или нужен read-back |
| executing | ready | доказано, что side effect не начинался, retry разрешён |
| executing | unknown_outcome | execution могло произойти, результат неизвестен |
| verifying | succeeded | expected actual state подтверждён |
| verifying | ready | желаемое состояние не достигнуто, side effect известен и retry допустим |
| verifying | failed | terminal failure подтверждён |
| verifying | unknown_outcome | verification inconclusive |
| unknown_outcome | verifying | начат reconciliation |
| planned | cancelled | отменено до execution |
| ready | cancelled | подтверждено отсутствие active side effect |

Запрещены прямые переходы:

- `unknown_outcome -> ready`;
- `unknown_outcome -> executing`;
- `executing -> succeeded` только по worker claim;
- `executing -> failed` только по transport timeout;
- cancel request → `cancelled` без evidence.

## Attempt state

Attempt states:

- `created`;
- `dispatched`;
- `accepted`;
- `running`;
- `reported`;
- `rejected`;
- `not_started`;
- `uncertain`;
- `closed`.

Attempt может быть closed, а operation — снова `ready` для следующего retry.

Новый retry сохраняет `operation_id`, но создаёт новый `attempt_id`, новую lease generation и `retry_of_attempt_id`.

## Known not-started

Worker rejection или transport evidence могут вернуть operation из `executing` в `ready` только если достаточно доказано, что side effect не мог начаться.

Если команда могла быть принята/выполнена до потери подтверждения, используется `unknown_outcome`.

## Unknown outcome

Состояние обязательно, когда:

1. operation могла изменить внешний target;
2. нет достаточного evidence expected state либо confirmed no-effect;
3. transport/worker report не устанавливает фактический результат.

В `unknown_outcome`:

- новый mutation retry запрещён;
- разрешены read-only reconciliation actions;
- допускаются escalation и bounded deferred verification;
- выход только через `verifying`.

## Verification verdict

Базовые verdict:

- `satisfied`;
- `not_satisfied_retryable`;
- `not_satisfied_terminal`;
- `inconclusive`.

Mapping:

- `satisfied -> succeeded`;
- `not_satisfied_retryable -> ready`;
- `not_satisfied_terminal -> failed`;
- `inconclusive -> unknown_outcome`.

## Task completion policy

По умолчанию:

- `completed` — все required operations succeeded либо явно признаны unnecessary/resolved decision record;
- `failed` — terminal required failure делает goal недостижимым;
- `blocked` — отсутствует prerequisite, но goal не признан terminal failure;
- `awaiting_decision` — создан explicit escalation/decision request.

Task state не выводится только из последнего сообщения.

## Issues

Issue states:

- `open`;
- `deferred`;
- `awaiting_decision`;
- `resolved`;
- `closed_unresolved`.

Issue lifecycle отделён от parent task/operation. Влияние на parent оформляется отдельным explicit transition/decision.

## Escalation

Escalation states:

- `requested`;
- `accepted`;
- `resolved`;
- `declined`;
- `expired`.

Escalation не является retry и не переносит execution ownership скрыто.

## Ownership и leases

Для serial mutation одновременно authoritative только одна lease generation.

Lease содержит:

- `lease_id`;
- `generation`;
- `operation_id`;
- `attempt_id`;
- `owner_actor_id`;
- validity metadata.

Event от stale generation:

- не меняет canonical state автоматически;
- сохраняется как evidence;
- может инициировать verification/unknown-outcome reconciliation.

Lease expiry прекращает authority worker на новые canonical transitions, но не доказывает остановку уже начатой mutation.

Поэтому новый mutation attempt не может получить active lease только на основании expiry. До re-dispatch требуется подтвердить, что старый executor больше не способен изменить target, либо применить target-level fencing/idempotency mechanism, делающий stale execution безопасным.

## Revision и concurrency

Каждый canonical transition увеличивает `revision`.

Решение применяется как compare-and-set:

- expected revision совпадает → transition допустим;
- revision изменился → решение пересчитывается на новом actual state.

Глобальная сериализация tasks не допускается как архитектурное предположение.

Для разных mutation operations используется domain-defined opaque `conflict_scope`:

- пересекающиеся scopes по умолчанию не имеют одновременно active mutation leases;
- scope вычисляется/валидируется доверенным control-side domain policy/adapter, а не принимается как authoritative worker input;
- explicit concurrent-safe policy может разрешить параллельность;
- если scope для двух mutations над одним известным target неизвестен, применяется консервативная сериализация либо escalation;
- concrete encoding conflict key не является частью state model и может быть выбран на R2, но семантика взаимного исключения является частью R1.

## Cancellation

Cancel request не является transition сама по себе.

- planned/ready + no active attempt → `cancelled`;
- executing + confirmed not-started/no-side-effect → `cancelled`;
- executing + side effect возможен → `unknown_outcome` либо bounded ожидание подтверждения;
- terminal state не переписывается поздним cancel request.

## Restart semantics

После restart persisted state должен отличать:

- terminal verified operations;
- ready operations;
- executing operations, требующие recovery evaluation;
- unknown outcomes;
- tasks awaiting decision.

Restart не создаёт retry автоматически. Если execution certainty потеряна, operation становится `unknown_outcome`, а не `failed`.

## Примеры trace

Successful:

`planned -> ready -> executing -> verifying -> succeeded`

Worker rejects before execution:

`ready -> executing -> ready`

Disconnect after possible mutation:

`ready -> executing -> unknown_outcome -> verifying -> succeeded|ready|failed|unknown_outcome`

Known failed attempt with retry:

`ready -> executing -> verifying -> ready -> executing ...`

Cancellation before start:

`planned|ready -> cancelled`

Duplicate/stale event:

canonical state не меняется; event сохраняется как evidence.

## Инварианты

1. Unknown outcome никогда не разрешает прямой retry.
2. Worker claim не переводит mutation напрямую в succeeded.
3. Transport failure не переводит mutation напрямую в failed.
4. Для serial mutation authoritative только одна lease generation.
5. Stale event не теряется и не становится автоматически authoritative.
6. Terminal state не переписывается поздним сообщением без explicit reconciliation record.
7. Revision растёт монотонно.
8. Task completion требует отсутствия unresolved required unknown outcomes.

## Отложенные решения

R1 не определяет persistence engine, lease duration, heartbeat, conflict-key schema, scheduler, retry budgets, daemon topology и wire-level transaction mechanism.
