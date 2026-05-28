# src/utils/lineage.py
from __future__ import annotations
import os, json, hashlib
from datetime import datetime, timezone
from .path_utils import get_lineage_output_path

def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def get_run_id() -> str:
    rid = os.environ.get("RUN_ID")
    if not rid:
        rid = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        os.environ["RUN_ID"] = rid
    return rid

def lineage_dir(stage: str, run_id: str | None = None) -> str:
    rid = run_id or get_run_id()
    d = get_lineage_output_path(stage, f"run_id={rid}")
    # get_lineage_output_path already mkdir - nothing else needed
    return d

def write_run_manifest(stage: str, manifest: dict, run_id: str | None = None, filename: str = "run.json") -> str:
    rid = run_id or get_run_id()
    d = lineage_dir(stage, rid)
    p = os.path.join(d, filename)
    if not os.path.exists(p):
        m = dict(manifest)
        m.setdefault("run_id", rid)
        m.setdefault("started_at", _utc_now())
        with open(p, "w", encoding="utf-8") as f:
            json.dump(m, f, indent=2)
    return p

def write_event(stage: str, event: dict, run_id: str | None = None, filename: str = "events.jsonl") -> str:
    rid = run_id or get_run_id()
    d = lineage_dir(stage, rid)
    p = os.path.join(d, filename)
    ev = dict(event)
    ev.setdefault("ts", _utc_now())
    ev.setdefault("run_id", rid)
    with open(p, "a", encoding="utf-8") as f:
        f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    return p

def schema_fingerprint(df) -> str:
    # Stable digest of schema (name:type)
    parts = []
    for f in df.schema.fields:
        try:
            parts.append(f"{f.name}:{f.dataType.simpleString()}")
        except Exception:
            parts.append(str(f))
    s = "|".join(sorted(parts))
    return hashlib.sha1(s.encode("utf-8")).hexdigest()
