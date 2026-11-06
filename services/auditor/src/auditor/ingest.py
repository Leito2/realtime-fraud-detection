"""One batch of Kafka messages -> grouped inserts. A store failure propagates: offsets stay uncommitted."""
from collections import defaultdict
from dataclasses import dataclass
from typing import Protocol

from auditor.rows import BadMessage, to_row


class Store(Protocol):
    def write(self, table: str, rows: list[dict]) -> None: ...


@dataclass(frozen=True)
class IngestResult:
    written: int
    bad: int


def ingest(store: Store, messages: list[tuple[str, bytes]]) -> IngestResult:
    by_table: dict[str, list[dict]] = defaultdict(list)
    bad = 0
    for topic, raw in messages:
        try:
            table, row = to_row(topic, raw)
        except BadMessage:
            bad += 1
            continue
        by_table[table].append(row)
    for table, rows in by_table.items():
        store.write(table, rows)
    return IngestResult(written=sum(len(r) for r in by_table.values()), bad=bad)
