import logging
import sys
from datetime import datetime, timezone

class _JsonishFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        base = {
            "ts": ts,
            "lvl": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Attach extras if provided (e.g., run_id, table)
        for k, v in getattr(record, "__dict__", {}).items():
            if k not in ("args", "msg", "levelname", "levelno", "name"):
                # keep a small whitelist of custom keys
                if k in ("run_id", "table", "job", "path"):
                    base[k] = v
        return " ".join(f'{k}="{v}"' for k, v in base.items())

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:  # avoid duplicate handlers in notebooks/REPL
        return logger
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(_JsonishFormatter())
    logger.addHandler(h)
    logger.propagate = False
    return logger
