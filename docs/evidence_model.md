---
document_type: evidence_model
document_version: 1.0
status: active
updated_at: 2026-09-15
---

# Evidence model v1

## Назначение

Evidence model отделяет заявленный результат от наблюдения, проверки actual state и canonical state decision.

Главный принцип: сообщение worker/transport не является само по себе доказательством фактического результата mutation.

## Понятия

- **Claim** — утверждение actor о результате.
- **Evidence** — неизменяемая запись наблюдаемого факта или артефакта.
- **Verification** — оценка evidence относительно expected state predicate.
- **Decision** — canonical transition, основанный на verification verdict и state policy.

## Evidence record

Минимальная логическая структура:

```json
{
  "evidence_id": "ev_...",
  "task_id": "tsk_...",
  "operation_id": "op_...",
  "attempt_id": "att_...",
  "kind": "target_observation",
  "source": {
    "actor_id": "actor_...",
    "source_type": "verifier"
  },
  "observed_at": "2026-09-15T20:10:00Z",
  "subject": {
    "type": "target_resource",
    "ref": "opaque-target-ref"
  },
  "summary": "Expected state observed",
  "artifact_ref": null,
  "digest": null
}
```

Record immutable после принятия. Correction создаёт новый record с relation к предыдущему.

## Evidence kinds

Базовые классы:

- `delivery_receipt`;
- `worker_report`;
- `provider_report`;
- `transport_observation`;
- `target_observation`;
- `process_observation`;
- `artifact_observation`;
- `verification_result`;
- `user_decision`;
- `policy_decision`.

Kind не определяет достоверность сам по себе.

## Достаточность evidence

v1 не использует универсальную числовую confidence score.

Достаточность задаёт verification policy конкретной operation:

- delivery receipt подтверждает delivery, но не execution;
- worker report подтверждает claim worker, но не target state;
- target read-back может подтверждать actual state соответствующего target layer;
- user decision разрешает действие, но не доказывает внешний side effect.

## Verification policy

Каждая significant mutation должна иметь:

- expected state predicate;
- verifier boundary;
- минимально достаточные evidence kinds;
- правила stale/conflicting evidence;
- policy id/version;
- mapping verdict в state model.

Domain/adapter может поставлять policy, но control plane сохраняет её identity/version вместе с решением.

## Verification verdict

Базовые verdict:

- `satisfied`;
- `not_satisfied_retryable`;
- `not_satisfied_terminal`;
- `inconclusive`.

Verification record содержит:

- `verification_id`;
- task/operation/attempt refs;
- policy id/version;
- использованные evidence ids;
- expected predicate summary;
- actual-state summary;
- verdict;
- verifier actor;
- timestamp.

Canonical transition ссылается на verification record.

## Claimed result и verified result

Worker может сообщить success, failure, partial result или uncertainty. Это worker report.

Для mutation:

- reported success обычно переводит operation в `verifying`;
- reported failure не доказывает отсутствие side effect;
- transport error не доказывает failure;
- contradictory reports требуют verification.

Исключение допустимо только если domain явно определяет worker-local state как authoritative target и verification policy это фиксирует.

## Actual-state read-back

Для significant mutation предпочтительный путь — read-back того слоя, который менялся.

Примеры:

- remote repository mutation → read-back repository state;
- filesystem mutation → чтение фактического файла/metadata на target host;
- service configuration → effective config/service state;
- deployment → deployed version и runtime health.

Проверяется поведение/состояние, а не только exit code изменяющей команды.

Сам факт, что desired state наблюдается после attempt, не всегда доказывает причинность именно этого attempt: target мог быть изменён параллельным actor или внешней системой. Verification policy должна явно определить, достаточно ли state satisfaction для цели operation или требуется causal attribution (например, target revision, request/idempotency key, audit event или иной domain-specific marker). При требуемой, но недоказанной причинности verdict остаётся `inconclusive` либо policy-defined non-success.

## Unknown outcome reconciliation

Для `unknown_outcome` verifier:

1. не повторяет mutation;
2. собирает read-only evidence;
3. проверяет actual target;
4. возвращает verdict;
5. при `inconclusive` сохраняет unknown outcome и может инициировать deferred/escalation path.

Отсутствие evidence не считается evidence failure.

## Duplicate evidence

Duplicate может определяться source-specific fingerprint/digest или explicit relation.

Повтор одной и той же записи не усиливает verification количеством копий.

## Conflicting evidence

Conflicting evidence не разрешается правилом «последнее сообщение побеждает».

Verifier оценивает:

- source layer;
- freshness;
- target revision/version;
- expected predicate;
- provenance.

Если конфликт нельзя разрешить безопасно, verdict = `inconclusive`.

## Freshness

Evidence может содержать:

- `observed_at`;
- optional `valid_until`;
- target revision/version;
- source revision.

Старое evidence не подтверждает актуальное состояние, если target мог измениться после observation.

## Artifacts

Large logs/files могут храниться отдельно через `artifact_ref` с:

- location abstraction;
- digest;
- size/type;
- retention class.

Reference не должен содержать чувствительные bearer-параметры. Digest подтверждает целостность артефакта, но не истинность его содержимого.

## Receipts

Receipt подтверждает факт на конкретном boundary:

- transport accepted message;
- worker accepted attempt;
- target API accepted request.

Receipt не равен successful outcome. Принятый/queued request всё равно требует проверки фактического состояния, если operation изменяющая.

## Provenance

Каждый evidence record сохраняет:

- actor/source;
- collection method class;
- timestamp;
- subject;
- relation to task/operation/attempt;
- optional artifact digest.

Derived verification record перечисляет source evidence ids.

## User decisions

User decision может:

- разрешить risky retry;
- принять partial result;
- отменить operation;
- выбрать escalation path.

Но user decision не подменяет actual-state evidence.

Если пользователь принимает unresolved unknown outcome, history фиксирует explicit risk acceptance; unknown result не переписывается как verified success/failure.

## Data minimization

Evidence хранит только необходимое для проверки и аудита.

Чувствительные значения и лишние private fragments удаляются до persistence. Если privileged access нужен для verification, он используется в отдельном execution context и не копируется в evidence.

## Retention

Evidence имеет bounded retention class, например:

- ephemeral diagnostic;
- task-lifetime;
- verification-critical;
- audit-required.

Конкретные сроки выбираются позднее. GC не должен удалить материал, необходимый для понимания terminal/unknown decision, пока соответствующий owner record сохраняется.

## Restart

После restart persisted evidence должно позволять восстановить:

- почему operation имеет текущий state;
- какие claims получены;
- какой actual-state verification выполнен;
- какой verdict привёл к terminal/unknown state.

Если evidence, нужное для безопасного retry, потеряно, retry-safe автоматически не предполагается.

## Минимальные требования по классам операций

### Read-only

Результат read может быть одновременно observation, если verifier boundary совпадает с target boundary.

### Mutation

По умолчанию требует post-mutation actual-state verification.

### Irreversible/high-blast-radius mutation

Должна поддерживать pre-state evidence, explicit authorization при необходимости и post-state verification. Конкретная risk policy принадлежит будущей integration с safety layer.

## Инварианты

1. Claim не равен verification.
2. Delivery receipt не равен execution result.
3. Exit code не равен actual state.
4. Conflicting evidence не разрешается скрыто.
5. Insufficient evidence сохраняет unknown outcome.
6. Verification ссылается на evidence ids и policy version.
7. Provenance сохраняется.
8. Диагностика не оправдывает перенос чувствительных значений в evidence.

## Отложенные решения

R1 не выбирает evidence storage engine, artifact store, retention durations, cryptographic attestation, domain-specific verifiers и audit export format.
