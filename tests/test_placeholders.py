import json
from pathlib import Path

from evalsuite.pipeline.sql_sanitize import has_placeholders

ROOT = Path(__file__).resolve().parents[1]


def _scan_jsonl(path: Path):
    issues = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        # Tasks use "question" field (not "prompt")
        if obj.get("question") is None and obj.get("prompt") is None:
            issues.append(f"{path}: question/prompt null")
        if obj.get("sql") == "":
            issues.append(f"{path}: sql empty")
        if "placeholder_tpcds" in line:
            issues.append(f"{path}: placeholder_tpcds")
    return issues


def test_has_placeholders():
    assert has_placeholders("SELECT 1") == (False, None)
    assert has_placeholders("")[0] is True
    assert has_placeholders("  \n  ")[0] is True
    ok, reason = has_placeholders("WHERE state = <manufacturer_id>")
    assert ok is True and "placeholder" in (reason or "")
    ok, _ = has_placeholders("WHERE state = YourState")
    assert ok is True
    ok, _ = has_placeholders("Replace with actual value")
    assert ok is True


def test_no_placeholders():
    data_root = ROOT / "data"
    if not data_root.exists():
        return  # skip when datasets are not prepared
    # Only scan prepared data directories, not raw source datasets
    prepared_dirs = ["bird", "spider2", "tpcds"]
    issues = []
    for d in prepared_dirs:
        dir_path = data_root / d
        if not dir_path.exists():
            continue
        for jsonl in dir_path.rglob("*.jsonl"):
            issues.extend(_scan_jsonl(jsonl))
    assert not issues, "Found placeholder/empty entries:\n" + "\n".join(issues)
