# src/utils/log_utils.py
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone

# cache so we don't add handlers multiple times
_LOGGERS: dict[str, logging.Logger] = {}


# ---------- paths ----------

def _project_root() -> Path:
    """
    src/utils/log_utils.py -> src/utils -> src -> <root>
    """
    return Path(__file__).resolve().parents[2]


def _log_dir() -> Path:
    """
    Where to write log files. Override with LOG_DIR env var.
    Default: <root>/reports/logs
    """
    d = os.getenv("LOG_DIR")
    p = Path(d) if d else (_project_root() / "reports" / "logs")
    p.mkdir(parents=True, exist_ok=True)
    return p


# ---------- formatting ----------

class _JsonishFormatter(logging.Formatter):
    """
    Console/file formatter: key="value" pairs on one line.
    Always UTC timestamps. Whitelists some extra fields passed via logger(..., extra={...}).
    """

    _EXTRA_KEEP = {"run_id", "table", "job", "path", "stage", "duration_ms"}

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        base = {
            "ts": ts,
            "lvl": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        # extras passed via logger(..., extra={...})
        for k in self._EXTRA_KEEP:
            if hasattr(record, k):
                base[k] = getattr(record, k)

        # include simple exception info if present
        if record.exc_info:
            base["exc"] = self.formatException(record.exc_info).splitlines()[-1][:300]

        # lightweight escaping for quotes/newlines
        def esc(v: object) -> str:
            s = str(v).replace('"', '\\"').replace("\n", "\\n")
            return s

        return " ".join(f'{k}="{esc(v)}"' for k, v in base.items())


# ---------- public API ----------

def get_logger(name: str, level: str | int | None = None) -> logging.Logger:
    """
    Create/retrieve a logger that logs to both console and a rotating file:
      - Console: key="value" style (UTC)
      - File   : <LOG_DIR or <root>/reports/logs>/{name}.log (rotates at 5MB, keep 5)

    Usage:
        logger = get_logger("connections.jdbc")
        logger.info("Spark session created", extra={"run_id": run_id, "table": "orders"})
    """
    if name in _LOGGERS:
        return _LOGGERS[name]

    logger = logging.getLogger(name)
    logger.setLevel(level or os.getenv("LOG_LEVEL", "INFO"))
    logger.propagate = False  # avoid duplicate emission via root

    # ---- Console handler (stdout) ----
    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setFormatter(_JsonishFormatter())
    logger.addHandler(sh)

    # ---- File handler (rotating) ----
    log_file = _log_dir() / f"{name}.log"
    fh = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    fh.setFormatter(_JsonishFormatter())
    logger.addHandler(fh)

    _LOGGERS[name] = logger
    return logger





















# import logging
# import sys
# from datetime import datetime, timezone

# class _JsonishFormatter(logging.Formatter):
#     def format(self, record: logging.LogRecord) -> str:
#         ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
#         base = {
#             "ts": ts,
#             "lvl": record.levelname,
#             "logger": record.name,
#             "msg": record.getMessage(),
#         }
#         # Attach extras if provided (e.g., run_id, table)
#         for k, v in getattr(record, "__dict__", {}).items():
#             if k not in ("args", "msg", "levelname", "levelno", "name"):
#                 # keep a small whitelist of custom keys
#                 if k in ("run_id", "table", "job", "path"):
#                     base[k] = v
#         return " ".join(f'{k}="{v}"' for k, v in base.items())

# def get_logger(name: str) -> logging.Logger:
#     logger = logging.getLogger(name)
#     if logger.handlers:  # avoid duplicate handlers in notebooks/REPL
#         return logger
#     logger.setLevel(logging.INFO)
#     h = logging.StreamHandler(sys.stdout)
#     h.setFormatter(_JsonishFormatter())
#     logger.addHandler(h)
#     logger.propagate = False
#     return logger
