import os
from typing import List, Dict
import clickhouse_connect
from backend.models import LogEntry


def _client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DB", "default"),
    )


def add_log(entry: LogEntry) -> None:
    client = _client()

    client.insert(
        table="logs",
        data=[[
            entry.timestamp,
            entry.prompt_name,
            entry.mr_url,
            entry.tokens_used,
            entry.time_seconds,
            entry.summary,
        ]],
        column_names=[
            "timestamp",
            "prompt_name",
            "mr_url",
            "tokens_used",
            "time_seconds",
            "summary",
        ],
    )


def get_logs() -> List[Dict]:
    client = _client()

    query = """
        SELECT
            timestamp,
            prompt_name,
            mr_url,
            tokens_used,
            time_seconds,
            summary
        FROM logs
        ORDER BY timestamp DESC
    """

    result = client.query(query)

    if not result.result_rows:
        return []

    columns = result.column_names
    rows = result.result_rows

    return [dict(zip(columns, row)) for row in rows]


def logs_to_csv() -> str:
    logs = get_logs()

    if not logs:
        return "timestamp,prompt_name,mr_url,tokens_used,time_seconds,summary\n"

    header = "timestamp,prompt_name,mr_url,tokens_used,time_seconds,summary"
    rows = [header]

    for log in logs:
        row = ",".join(
            f'"{str(log[col])}"'
            for col in [
                "timestamp",
                "prompt_name",
                "mr_url",
                "tokens_used",
                "time_seconds",
                "summary",
            ]
        )
        rows.append(row)

    return "\n".join(rows) + "\n"