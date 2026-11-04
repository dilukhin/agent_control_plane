"""Подготовка искусственного репозитория и вызов настоящего адаптера ScopedKB."""

from dataclasses import asdict
import json
import os
from pathlib import Path
import subprocess
import sys

from scopedkb.review_git import Grant, ReviewGit, candidate_digest, encode

REF = "refs/scopedkb/proposals/acp-review-proof"
DESTINATION = "repository:example-common"
AUTHOR = "subject:agent:example-agent"
KEY = "proposal:00000000-0000-4000-8000-987654321abc"


def git(repository, *args, data=None):
    env = {key: val for key, val in os.environ.items() if not key.upper().startswith("GIT_")}
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
    return subprocess.run(
        ["git", "--git-dir=" + str(repository), *args], env=env, input=data, timeout=5,
        capture_output=True, check=True,
    ).stdout.strip()


def seed(repository):
    fixtures = Path(__file__).parent
    base = json.loads((fixtures / "base.json").read_bytes())
    complete = json.loads((fixtures / "candidate.json").read_bytes())
    base["artifacts"] = complete["artifacts"]
    recommendation = next(f for f in complete["facts"] if f["status"] == "candidate")
    proposal = {
        "schema_version": "2.0.0", "id": KEY, "type": "proposal", "scope": "common",
        "sensitivity": "public", "tags": ["fixture"], "proposed_fact": recommendation["id"],
        "destination_repository": DESTINATION, "author": AUTHOR,
        "evidence": recommendation["verification"]["evidence"],
        "destination_validation": {"status": "passed", "policy_snapshot_sha256": "1" * 64, "issue_codes": []},
        "review_status": "pending", "created_at": "2026-01-03T00:00:00Z",
    }
    blob = git(repository, "hash-object", "-w", "--stdin", data=encode(base))
    tree = git(repository, "mktree", data=b"100644 blob " + blob + b"\tdataset.json\n")
    revision = git(repository, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                   "commit-tree", tree.decode(), "-m", "Synthetic dataset").decode()
    for ref in ("refs/heads/main", REF):
        git(repository, "update-ref", ref, revision)
    return revision, encode({"fact": recommendation, "proposal": proposal, "relations": complete["relations"]})


def main():
    mode, workspace, scopedkb = sys.argv[1:]
    root = Path(workspace)
    repository = root / "scopedkb.git"
    plan_path = root / "plan.json"
    payload_path = root / "request.json"
    if mode == "seed":
        git(repository, "init", "--bare", "--template=", "--object-format=sha1")
        revision, candidate = seed(repository)
        payload_path.write_bytes(candidate)
        plan_path.write_bytes(encode({
            "Version": 1, "Target": "scopedkb-fixture/" + KEY,
            "BaseDigest": revision, "ResultDigest": candidate_digest(candidate),
        }))
        return
    store = ReviewGit(Grant(repository, REF, DESTINATION, AUTHOR), Path(scopedkb) / "schemas" / "v2")
    plan = json.loads(plan_path.read_bytes())
    if mode == "write":
        candidate = payload_path.read_bytes()
        if candidate_digest(candidate) != plan["ResultDigest"]:
            raise ValueError("Изменились входные байты кандидата.")
        store.append(plan["BaseDigest"], candidate)
    elif mode == "lookup":
        observation = store.lookup(KEY)
        print(json.dumps(asdict(observation) | {
            "main": git(repository, "rev-parse", "refs/heads/main").decode(),
            "parent": git(repository, "rev-parse", observation.revision + "^").decode()
            if observation.digest is not None else None,
        }))
    else:
        raise ValueError("Неизвестный этап проверки.")


if __name__ == "__main__":
    main()
