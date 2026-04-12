from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

from evalsuite.core.config import Config, save_config_json
from evalsuite.core.types import BenchSummary, TaskResult


def generate_run_id(model_name: str) -> str:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_model = model_name.replace("/", "_")
    return f"{ts}_{safe_model}"


def ensure_run_dir(output_root: Path, run_id: str) -> Path:
    run_dir = output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "raw").mkdir(exist_ok=True)
    return run_dir


def list_runs(output_root: Path) -> list[str]:
    if not output_root.exists():
        return []
    return sorted([p.name for p in output_root.iterdir() if p.is_dir()])


def save_config(config: Config, run_dir: Path, effective_model_name: str | None = None) -> None:
    save_config_json(config, run_dir / "config.json", effective_model_name=effective_model_name)


def save_run_config(run_config: dict, run_dir: Path) -> None:
    (run_dir / "run_config.json").write_text(json.dumps(run_config, indent=2))


def append_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        # Use default=str to serialize Decimal and other non-JSON primitives.
        f.write(json.dumps(record, default=str) + "\n")


def write_event(run_dir: Path, event: dict) -> None:
    append_jsonl(run_dir / "events.jsonl", event)


def write_result(run_dir: Path, result: TaskResult) -> None:
    raw_dir = run_dir / "raw"
    raw_dir.mkdir(exist_ok=True)
    append_jsonl(
        raw_dir / f"{result.bench}.jsonl",
        {
            "task_id": result.task_id,
            "bench": result.bench,
            "gold_sql": result.gold_sql,
            "pred_sql": result.pred_sql,
            "prompt": result.prompt,
            "gold": result.gold.__dict__,
            "pred": result.pred.__dict__,
            "match": result.match,
            "status": result.status,
            "error_message": result.error_message,
            "error_type": result.error_type,
            "latency_ms": result.latency_ms,
            "timestamp": result.timestamp,
            "extra": result.extra,
        },
    )


def write_bench_summaries(run_dir: Path, summaries: Iterable[BenchSummary]) -> None:
    data = [s.__dict__ for s in summaries]
    overall = {
        "total": sum(s.total for s in summaries),
        "executed": sum(s.executed for s in summaries),
        "skipped": sum(s.skipped for s in summaries),
        "gold_failed": sum(s.gold_failed for s in summaries),
        "pred_failed": sum(s.pred_failed for s in summaries),
        "ex_correct": sum(s.ex_correct for s in summaries),
        "compared": sum(s.compared for s in summaries),
    }
    payload = {"benches": data, "overall": overall}
    (run_dir / "summary.json").write_text(json.dumps(payload, indent=2))


def write_report(run_dir: Path, report: dict, markdown: str) -> None:
    (run_dir / "report.json").write_text(json.dumps(report, indent=2))
    (run_dir / "report.md").write_text(markdown)


def load_completed_task_ids(run_dir: Path) -> set[str]:
    raw_dir = run_dir / "raw"
    if not raw_dir.exists():
        return set()
    completed: set[str] = set()
    for path in raw_dir.glob("*.jsonl"):
        with path.open() as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    completed.add(str(rec.get("task_id")))
                except json.JSONDecodeError:
                    continue
    return completed


def load_results(run_dir: Path) -> list[dict]:
    raw_dir = run_dir / "raw"
    if not raw_dir.exists():
        return []
    out: list[dict] = []
    for path in raw_dir.glob("*.jsonl"):
        with path.open() as f:
            for line in f:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return out
