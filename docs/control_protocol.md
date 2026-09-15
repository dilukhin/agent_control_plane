---
document_type: control_protocol
document_version: 0.1
protocol_version: "1.0"
status: proposed
updated_at: 2026-09-15
---

# Control protocol v1

## Назначение

Протокол определяет логический обмен control plane независимо от transport, языка, storage, worker/model provider и UI. Он читается совместно с `state_model.md`, `evidence_model.md` и `security_model.md`.

## Объекты

- **Task** — устойчивая цель верхнего уровня.
- **Operation** — ограниченная единица работы с одним логическим намерением.
- **Attempt** — одна конкретная попытка исполнения operation.
- **Message** — единица логического обмена.
- **Issue** — ортогональная проблема исполнения/диагностики.
- **Escalation** — явный запрос решения или передачи работы другому worker/model/user.

## Identity

Используются устойчивые opaque ID:

- `task_id`;
- `operation_id`;
- `attempt_id`;
- `message_id`;
- `lease_id`;
- `issue_id`;
- `escalation_id`;
- `evidence_id`.

Правила:

1. ID уникальны в сохраняемой истории и не переиспользуются.
2. ID не кодируют hostname, model, transport, topology или чувствительные данные.
3. `operation_id` сохраняется между retry одного логического намерения.
4. Новый semantic retry создаёт новый `attempt_id`.
5. Transport retransmission сохраняет исходные `message_id` и `attempt_id`.

## Correlation

Каждое сообщение несёт:

- `correlation_id` — identity логической цепочки;
- `causation_id` — `message_id` непосредственной причины, если она известна;
- ссылки на соответствующие task/operation/attempt.

Correlation не заменяет object identity и не предполагает единственный глобальный последовательный поток.

## Envelope

Логический envelope v1:

```json
{
  "protocol_version": "1.0",
  "message_id": "msg_...",
  "message_type": "command",
  "message_name": "operation.offer",
  "task_id": "tsk_...",
  "operation_id": "op_...",
  "attempt_id": "att_...",
  "correlation_id": "corr_...",
  "causation_id": "msg_...",
  "actor": {
    "actor_id": "actor_...",
    "actor_role": "controller"
  },
  "issued_at": "2026-09-15T20:00:00Z",
  "deadline_at": null,
  "lease": {
    "lease_id": "lease_...",
    "generation": 3
  },
  "payload": {}
}
```

Transport-specific metadata не становится canonical protocol state.

## Message types

Классы v1:

- `command`;
- `event`;
- `observation`;
- `decision_request`;
- `decision_response`.

Базовые names:

- `operation.offer`;
- `operation.cancel_request`;
- `operation.accepted`;
- `operation.rejected`;
- `operation.started`;
- `operation.progress`;
- `operation.result_reported`;
- `operation.execution_uncertain`;
- `verification.observed`;
- `verification.completed`;
- `issue.opened`, `issue.updated`, `issue.closed`;
- `escalation.requested`, `escalation.resolved`.

## Canonical state ownership

Canonical task/operation state принадлежит control plane.

Worker, provider и transport:

- отправляют events/observations;
- могут заявить result;
- не объявляют verified completion;
- не получают workflow ownership только из факта доставки или исполнения.

## Execution ownership

Для serial mutation control plane выдаёт lease:

- `lease_id`;
- `generation`;
- `owner_actor_id`;
- policy-defined validity.

Одновременно authoritative только одна generation. Event от stale lease сохраняется как evidence и не применяется автоматически как canonical transition.

Истечение lease не доказывает failure и не доказывает, что старый executor физически прекратил side effects. Перед выдачей нового mutation lease control plane должен установить хотя бы одно из условий:

- предыдущий executor подтверждённо quiescent/terminated и больше не способен изменить target;
- target/adapter поддерживает fencing token, который делает stale generation физически неспособной выполнить mutation;
- operation имеет подтверждённую target-level idempotency/concurrency semantics, безопасную для нового attempt.

Одного истечения lease или потери heartbeat недостаточно для re-dispatch mutation.

### Conflict scope между разными operations

Каждая изменяющая operation должна иметь domain-defined `conflict_scope`: один или несколько opaque keys, обозначающих ресурс/область, где параллельные mutations могут конфликтовать.

Правила v1:

- operations с пересекающимся `conflict_scope` не получают одновременно active mutation leases по умолчанию;
- параллельность разрешается только explicit policy, подтверждающей concurrent-safe semantics;
- `conflict_scope` не кодирует transport/provider identity;
- `conflict_scope` вычисляется/валидируется доверенным control-side domain policy или adapter; worker/provider не может сам выбрать scope, чтобы обойти взаимное исключение;
- если adapter не может надёжно определить scope для двух mutations над одним известным target, применяется консервативная сериализация либо explicit escalation;
- read-only verification не блокируется mutation scope, если domain policy не требует иного.

Конкретный формат ключа остаётся opaque для core; его стабильность и сравнимость задаёт domain adapter contract.

## Delivery semantics

v1 не обещает exactly-once delivery.

- transport может повторить, задержать или переупорядочить сообщения;
- duplicate определяется по `message_id`;
- semantic duplicate дополнительно определяется по operation/attempt/lease;
- ordering не выводится только из порядка прихода.

## Timeout

`deadline_at` ограничивает ожидание, но не доказывает отмену внешнего действия.

Если mutation могла быть доставлена или начата, timeout/disconnect приводит к `unknown_outcome`, а не к автоматическому failure или retry.

## Retry и idempotency

Transport retransmission — то же logical message.

Semantic retry:

- сохраняет `operation_id`;
- получает новый `attempt_id`, `message_id` и lease generation;
- фиксирует `retry_of_attempt_id`.

Retry разрешён только после проверки actual state, idempotency и ownership предыдущей попытки, а для mutation — также после доказательства execution quiescence либо наличия target-level fencing/idempotency, исключающих поздний конфликт старого attempt.

Если target поддерживает idempotency key, adapter может использовать стабильный ключ на основе `operation_id`; поддержка не предполагается по умолчанию.

## Cancellation

Cancel request — намерение, а не факт отмены.

Terminal cancelled допустим только при evidence, что side effect не начался либо был безопасно отменён. При неопределённости используется `unknown_outcome`.

## Issues и escalation

Issue имеет parent task и, при необходимости, parent operation. Она не меняет scope/состояние parent скрыто.

Escalation содержит:

- source task/operation/issue;
- `target_type`: worker, model или user;
- reason;
- requested decision/action;
- bounded context references.

Скрытый fallback не считается escalation.

## Compatibility

Canonical version: `1.0`.

1. Несовпадающий major version не исполняется автоматически.
2. Receiver возвращает explicit unsupported-version/message вместо угадывания.
3. Новые optional поля допустимы, если их отсутствие не меняет исходную семантику.
4. Изменение смысла существующего поля требует новой protocol version.
5. Custom extensions размещаются только в namespaced `extensions`.
6. Persisted message сохраняет исходный `protocol_version`.

## Extensions

Extension:

- не меняет canonical state semantics;
- не обязательна для понимания base v1;
- имеет явный namespace owner.

Если extension становится обязательной для core semantics, это protocol change.

## Security constraint

Canonical message не переносит секретные значения. При будущей необходимости допускается только opaque reference на отдельный privileged mechanism.

Actor identity в envelope не является credential; authentication принадлежит boundary/adapter.

## Обязательные сценарии

- successful mutation: worker report → verification → подтверждение actual state → success;
- timeout/disconnect: возможная mutation → unknown outcome → reconciliation;
- duplicate delivery: повтор message не создаёт новый attempt;
- worker loss: lease expiry не доказывает failure;
- parallel tasks: независимые identity/correlation, без глобальной сериализации;
- version mismatch: несовместимая команда не исполняется.

## Отложенные решения

v1 не выбирает wire encoding, HTTP/WebSocket/IPC/relay transport, authentication mechanism, concrete worker/provider API, storage, implementation language, signing и model routing policy.
