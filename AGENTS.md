# AGENTS.md

Перед изменением репозитория прочитай `README.md` и канонический baseline `docs/project_baseline.md`.

## Правила работы

- Выполняй только явно поставленную bounded task; неожиданную архитектурную развилку возвращай в control plane с evidence, а не решай скрыто.
- Не создавай roadmap и не расширяй scope, если это не является текущей задачей.
- Не добавляй language/framework, dependencies, package manager, database, daemon/client/server, protocol implementation, model router или CI «на будущее».
- Сохраняй границы с `opencode_setup`, `ssh_relay`, `agent-safe` и `github-connector-knowledge`; общий механизм не дублируй project-specific реализацией без доказанной необходимости.
- Для mutation сначала фиксируй target/expected state, затем делай минимальное изменение и проверяй actual state. Timeout/disconnect означает unknown outcome до проверки.
- Не помещай secrets, credentials, private keys, passwords, passphrases, authorization headers, cookies или relay/session tokens в код, task payloads, логи, receipts/evidence и Git.
- Не затрагивай посторонние пользовательские изменения и не применяй destructive recovery без прямого разрешения.
- Используй GitHub transport, явно назначенный текущей средой/задачей; не делай скрытый fallback на другой transport.

Если конкретная задача требует отступить от baseline, это должно быть явным решением с обновлением документа-владельца либо отдельным зафиксированным design decision.
