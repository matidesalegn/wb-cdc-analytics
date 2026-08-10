"""Structured logging for tasks.

One JSON object per line, keys sorted, None values dropped. That is a deliberately
small amount of machinery for a real benefit: a log line that is parseable can be
queried, and Airflow task logs are otherwise the least searchable part of a pipeline.

Sorted keys so a diff between two runs is readable. None dropped so an absent value is
absent rather than the string "None", which is the kind of thing that ends up in a
dashboard.
"""

from __future__ import annotations

import json
import logging
from typing import Any


def emit(logger: logging.Logger, event: str, level: int = logging.INFO, **fields: Any) -> None:
    payload = {"event": event, **{k: v for k, v in fields.items() if v is not None}}
    logger.log(level, json.dumps(payload, sort_keys=True, default=str))
