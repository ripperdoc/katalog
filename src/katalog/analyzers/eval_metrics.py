from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from collections import defaultdict

from loguru import logger

from katalog.analyzers.base import Analyzer, AnalyzerResult, AnalyzerScope
from katalog.analyzers.utils import build_scoped_assets_cte
from katalog.config import current_workspace
from katalog.constants.metadata import (
    ASSET_CANONICAL_URI,
    EVAL_AVG_SENTENCE_WORDS,
    EVAL_COMPLETENESS,
    EVAL_SENTENCE_COUNT,
    EVAL_SIMILARITY,
    EVAL_UNIQUE_WORD_RATIO,
    FILE_TYPE,
    get_metadata_id,
)
from katalog.db.sqlspec import session_scope
from katalog.db.sqlspec.sql_helpers import select
from katalog.db.sqlspec.tables import ASSET_TABLE, METADATA_TABLE
from katalog.models import Changeset
from katalog.utils.exports import write_csv_tables


class EvalMetricsAnalyzer(Analyzer):
    """Aggregate eval metrics from per-asset metadata."""

    plugin_id = "katalog.analyzers.eval_metrics.EvalMetricsAnalyzer"
    title = "Eval metrics"
    description = "Aggregate and export parsing-eval metrics."
    output_kind = "eval_metrics"
    supports_single_asset = False

    def should_run(self, *, changeset: Changeset) -> bool:
        _ = changeset
        return True

    async def run(
        self, *, changeset: Changeset, scope: AnalyzerScope
    ) -> AnalyzerResult:
        if scope.kind == "asset":
            raise ValueError("Eval metrics analyzer does not support single-asset scope")

        scoped_cte, scoped_params = build_scoped_assets_cte(
            scope,
            asset_table=ASSET_TABLE,
            metadata_table=METADATA_TABLE,
        )
        key_similarity = get_metadata_id(EVAL_SIMILARITY)
        key_completeness = get_metadata_id(EVAL_COMPLETENESS)
        key_unique_ratio = get_metadata_id(EVAL_UNIQUE_WORD_RATIO)
        key_sentence_count = get_metadata_id(EVAL_SENTENCE_COUNT)
        key_avg_sentence_words = get_metadata_id(EVAL_AVG_SENTENCE_WORDS)
        key_mime = get_metadata_id(FILE_TYPE)
        key_uri = get_metadata_id(ASSET_CANONICAL_URI)

        sql = f"""
        WITH {scoped_cte},
        current_metadata AS (
            SELECT
                m.asset_id,
                m.metadata_key_id,
                m.value_text,
                m.value_int,
                m.value_real,
                ROW_NUMBER() OVER (
                    PARTITION BY m.asset_id, m.metadata_key_id
                    ORDER BY m.changeset_id DESC, m.id DESC
                ) AS rn
            FROM {METADATA_TABLE} m
            JOIN scoped_assets s ON s.asset_id = m.asset_id
            WHERE m.removed = 0
              AND m.metadata_key_id IN (?, ?, ?, ?, ?, ?, ?)
        )
        SELECT
            a.id AS asset_id,
            COALESCE(
                MAX(CASE WHEN cm.metadata_key_id = ? THEN cm.value_text END),
                a.canonical_uri
            ) AS canonical_uri,
            MAX(CASE WHEN cm.metadata_key_id = ? THEN cm.value_text END) AS mime,
            MAX(CASE WHEN cm.metadata_key_id = ? THEN cm.value_real END) AS similarity,
            MAX(CASE WHEN cm.metadata_key_id = ? THEN cm.value_real END) AS completeness,
            MAX(CASE WHEN cm.metadata_key_id = ? THEN cm.value_real END) AS unique_word_ratio,
            MAX(CASE WHEN cm.metadata_key_id = ? THEN cm.value_int END) AS sentence_count,
            MAX(CASE WHEN cm.metadata_key_id = ? THEN cm.value_real END) AS avg_sentence_words
        FROM {ASSET_TABLE} a
        JOIN scoped_assets s ON s.asset_id = a.id
        LEFT JOIN current_metadata cm
            ON cm.asset_id = a.id AND cm.rn = 1
        GROUP BY a.id
        ORDER BY a.id
        """
        params: list[Any] = [
            *scoped_params,
            key_similarity,
            key_completeness,
            key_unique_ratio,
            key_sentence_count,
            key_avg_sentence_words,
            key_mime,
            key_uri,
            key_uri,
            key_mime,
            key_similarity,
            key_completeness,
            key_unique_ratio,
            key_sentence_count,
            key_avg_sentence_words,
        ]

        async with session_scope(analysis=True) as session:
            rows = await select(session, sql, params)

        assets_total = len(rows)
        similarity_threshold = _similarity_threshold(self.actor.config)
        rows_with_similarity = [
            row
            for row in rows
            if row.get("similarity") is not None and float(row["similarity"]) > 0.0
        ]
        failed_rows = [
            row
            for row in rows_with_similarity
            if float(row.get("similarity") or 0.0) < similarity_threshold
        ]
        avg_similarity = _avg(rows_with_similarity, "similarity")
        avg_completeness = _avg(rows_with_similarity, "completeness")
        avg_unique_ratio = _avg(rows, "unique_word_ratio")
        avg_sentence_words = _avg(rows, "avg_sentence_words")

        summary = {
            "assets_total": assets_total,
            "assets_with_similarity": len(rows_with_similarity),
            "avg_similarity": avg_similarity,
            "avg_completeness": avg_completeness,
            "avg_unique_word_ratio": avg_unique_ratio,
            "avg_sentence_words": avg_sentence_words,
        }

        mime_breakdown: dict[str, dict[str, float | int]] = {}
        for row in rows:
            mime = str(row.get("mime") or "unknown")
            bucket = mime_breakdown.setdefault(
                mime,
                {
                    "mime": mime,
                    "count": 0,
                    "avg_similarity": 0.0,
                    "avg_completeness": 0.0,
                    "avg_unique_word_ratio": 0.0,
                },
            )
            bucket["count"] = int(bucket["count"]) + 1

        for mime, bucket in mime_breakdown.items():
            mime_rows = [row for row in rows if str(row.get("mime") or "unknown") == mime]
            bucket["avg_similarity"] = _avg(mime_rows, "similarity")
            bucket["avg_completeness"] = _avg(mime_rows, "completeness")
            bucket["avg_unique_word_ratio"] = _avg(mime_rows, "unique_word_ratio")

        output = {
            "summary": summary,
            "breakdowns": {
                "mime": list(mime_breakdown.values()),
            },
        }

        table_rows = [
            {
                "asset_id": row.get("asset_id"),
                "canonical_uri": row.get("canonical_uri"),
                "mime": row.get("mime"),
                "similarity": row.get("similarity"),
                "completeness": row.get("completeness"),
                "unique_word_ratio": row.get("unique_word_ratio"),
                "sentence_count": row.get("sentence_count"),
                "avg_sentence_words": row.get("avg_sentence_words"),
            }
            for row in rows
        ]
        ingester_rows = [
            {
                "input": row.get("canonical_uri"),
                "mime": row.get("mime"),
                "similarity": row.get("similarity"),
                "completeness": row.get("completeness"),
                "unique_word_ratio": row.get("unique_word_ratio"),
                "sentences": row.get("sentence_count"),
                "sentence_length": row.get("avg_sentence_words"),
            }
            for row in rows
        ]
        csv_paths = write_csv_tables(
            {
                "eval_assets": table_rows,
                "eval_mime": list(mime_breakdown.values()),
            },
            prefix=f"changeset-{changeset.id}_actor-{self.actor.id}_eval",
        )
        extra_paths = _write_eval_text_reports(
            changeset_id=changeset.id,
            actor_id=self.actor.id,
            similarity_threshold=similarity_threshold,
            rows=rows,
            failed_rows=failed_rows,
            summary=summary,
            mime_breakdown=list(mime_breakdown.values()),
            ingestion_rows=ingester_rows,
        )
        if csv_paths:
            workspace = current_workspace()
            output["exports"] = {
                "csv": [
                    str(path.relative_to(workspace))
                    if workspace is not None and path.is_relative_to(workspace)
                    else str(path)
                    for path in csv_paths
                ]
            }
        if extra_paths:
            exports = dict(output.get("exports") or {})
            workspace = current_workspace()
            exports["text"] = [
                str(path.relative_to(workspace))
                if workspace is not None and path.is_relative_to(workspace)
                else str(path)
                for path in extra_paths
            ]
            output["exports"] = exports

        logger.info(
            "Eval metrics analyzer completed: assets={assets}, with_similarity={with_similarity}",
            assets=assets_total,
            with_similarity=len(rows_with_similarity),
        )
        return AnalyzerResult(output=output)


def _avg(rows: list[dict[str, Any]], key: str) -> float:
    values = []
    for row in rows:
        value = row.get(key)
        if value is None:
            continue
        try:
            values.append(float(value))
        except (TypeError, ValueError):
            continue
    if not values:
        return 0.0
    return sum(values) / len(values)


def _similarity_threshold(config: dict[str, Any] | None) -> float:
    if not config:
        return 80.0
    value = config.get("similarity_threshold")
    try:
        return float(value) if value is not None else 80.0
    except (TypeError, ValueError):
        return 80.0


def _write_eval_text_reports(
    *,
    changeset_id: int,
    actor_id: int | None,
    similarity_threshold: float,
    rows: list[dict[str, Any]],
    failed_rows: list[dict[str, Any]],
    summary: dict[str, Any],
    mime_breakdown: list[dict[str, Any]],
    ingestion_rows: list[dict[str, Any]],
) -> list[Path]:
    workspace = current_workspace()
    if workspace is None:
        raise ValueError("Workspace is not configured for eval exports")

    exports_dir = workspace / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)
    eval_latest_dir = workspace / "eval" / "latest"
    eval_latest_dir.mkdir(parents=True, exist_ok=True)
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    eval_run_dir = workspace / "eval" / "output" / f"eval_run_{run_timestamp}"
    eval_run_dir.mkdir(parents=True, exist_ok=True)
    actor_label = actor_id if actor_id is not None else "unknown"
    prefix = f"changeset-{changeset_id}_actor-{actor_label}_eval"
    run_info_path = exports_dir / f"{prefix}_run_info.txt"
    failure_path = exports_dir / f"{prefix}_failure_report.txt"
    format_metrics_path = exports_dir / f"{prefix}_format_metrics.csv"
    ingestion_eval_path = exports_dir / f"{prefix}_ingestion_eval.csv"

    run_info_lines = [
        "Evaluation Run Info",
        "===================",
        f"Timestamp: {datetime.now(timezone.utc).isoformat()}",
        f"Assets total: {summary.get('assets_total', 0)}",
        f"Assets with similarity: {summary.get('assets_with_similarity', 0)}",
        f"Average similarity: {summary.get('avg_similarity', 0.0):.2f}",
        f"Average completeness: {summary.get('avg_completeness', 0.0):.2f}",
        f"Average unique word ratio: {summary.get('avg_unique_word_ratio', 0.0):.4f}",
        f"Average sentence words: {summary.get('avg_sentence_words', 0.0):.2f}",
        f"Similarity threshold: {similarity_threshold:.1f}",
        f"Failures below threshold: {len(failed_rows)}",
    ]
    run_info_content = "\n".join(run_info_lines) + "\n"
    run_info_path.write_text(run_info_content, encoding="utf-8")
    (eval_latest_dir / "run_info.txt").write_text(run_info_content, encoding="utf-8")
    (eval_run_dir / "run_info.txt").write_text(run_info_content, encoding="utf-8")

    failure_lines = [
        "Failure Report",
        "==============",
        f"Similarity threshold: {similarity_threshold:.1f}",
        f"Failures: {len(failed_rows)}",
        "",
    ]
    if rows:
        total_docs = len([row for row in rows if _float_or_none(row.get("similarity"))])
        failure_lines.extend(
            [
                "Overall Statistics:",
                f"Total documents analyzed: {total_docs}",
                (
                    f"Total documents below threshold: {len(failed_rows)} "
                    f"({(len(failed_rows) / total_docs):.1%})"
                    if total_docs > 0
                    else "Total documents below threshold: 0 (0.0%)"
                ),
                "",
                "Breakdown by MIME type:",
                "----------------------",
            ]
        )
        grouped_failed: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in failed_rows:
            grouped_failed[str(row.get("mime") or "unknown")].append(row)
        for bucket in mime_breakdown:
            mime = str(bucket.get("mime") or "unknown")
            total_mime_docs = int(bucket.get("count") or 0)
            failures = grouped_failed.get(mime, [])
            if not failures:
                continue
            failure_rate = (len(failures) / total_mime_docs) if total_mime_docs > 0 else 0.0
            failure_lines.extend(
                [
                    "",
                    f"{mime}:",
                    f"- Total documents: {total_mime_docs}",
                    f"- Failed documents: {len(failures)} ({failure_rate:.1%})",
                    "- Failed document URIs:",
                ]
            )
            for row in failures:
                failure_lines.append(
                    "  * "
                    + (
                        f"{row.get('canonical_uri') or ''} "
                        f"(similarity: {_fmt_float(row.get('similarity'))}%, "
                        f"completeness: {_fmt_float(row.get('completeness'))}%, "
                        f"unique_word_ratio: {_fmt_float(row.get('unique_word_ratio'))}, "
                        f"sentences: {int(row.get('sentence_count') or 0)}, "
                        f"words_per_sentence: {_fmt_float(row.get('avg_sentence_words'))})"
                    )
                )
    for row in failed_rows:
        if rows:
            break
        failure_lines.append(
            " | ".join(
                [
                    f"asset_id={row.get('asset_id')}",
                    f"mime={row.get('mime') or 'unknown'}",
                    f"similarity={_fmt_float(row.get('similarity'))}",
                    f"completeness={_fmt_float(row.get('completeness'))}",
                    f"uri={row.get('canonical_uri') or ''}",
                ]
            )
        )
    failure_content = "\n".join(failure_lines) + "\n"
    failure_path.write_text(failure_content, encoding="utf-8")
    (eval_latest_dir / "failure_report.txt").write_text(
        failure_content, encoding="utf-8"
    )
    (eval_run_dir / "failure_report.txt").write_text(failure_content, encoding="utf-8")

    format_rows = _build_format_metrics_rows(rows)
    format_metrics_path_list = write_csv_tables(
        {"format_metrics": format_rows},
        prefix=prefix,
        directory=exports_dir,
    )
    if format_metrics_path_list:
        format_metrics_path = format_metrics_path_list[0]
    write_csv_tables(
        {"format_metrics": format_rows},
        prefix="format",
        directory=eval_latest_dir,
    )
    write_csv_tables(
        {"format_metrics": format_rows},
        prefix="format",
        directory=eval_run_dir,
    )
    latest_format = _rename_single_csv(eval_latest_dir, "format_format_metrics.csv", "format_metrics.csv")
    run_format = _rename_single_csv(eval_run_dir, "format_format_metrics.csv", "format_metrics.csv")

    ingestion_paths = write_csv_tables(
        {"ingestion_eval": ingestion_rows},
        prefix=prefix,
        directory=exports_dir,
    )
    if ingestion_paths:
        ingestion_eval_path = ingestion_paths[0]
    write_csv_tables(
        {"ingestion_eval": ingestion_rows},
        prefix="ingestion",
        directory=eval_latest_dir,
    )
    write_csv_tables(
        {"ingestion_eval": ingestion_rows},
        prefix="ingestion",
        directory=eval_run_dir,
    )
    latest_ingestion = _rename_single_csv(
        eval_latest_dir, "ingestion_ingestion_eval.csv", "ingestion_eval.csv"
    )
    run_ingestion = _rename_single_csv(
        eval_run_dir, "ingestion_ingestion_eval.csv", "ingestion_eval.csv"
    )

    paths = [run_info_path, failure_path]
    if format_metrics_path.exists():
        paths.append(format_metrics_path)
    if ingestion_eval_path.exists():
        paths.append(ingestion_eval_path)
    for path in [latest_format, run_format, latest_ingestion, run_ingestion]:
        if path is not None:
            paths.append(path)
    paths.extend([eval_latest_dir / "run_info.txt", eval_latest_dir / "failure_report.txt"])
    paths.extend([eval_run_dir / "run_info.txt", eval_run_dir / "failure_report.txt"])
    return paths


def _build_format_metrics_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_mime: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        mime = str(row.get("mime") or "unknown")
        by_mime[mime].append(row)
    formatted: list[dict[str, Any]] = []
    for mime in sorted(by_mime.keys()):
        mime_rows = by_mime[mime]
        similarity_values = [
            value / 100.0
            for value in (_float_or_none(row.get("similarity")) for row in mime_rows)
            if value is not None and value > 0.0
        ]
        completeness_values = [
            value / 100.0
            for value in (_float_or_none(row.get("completeness")) for row in mime_rows)
            if value is not None and value > 0.0
        ]
        unique_values = [
            value
            for value in (_float_or_none(row.get("unique_word_ratio")) for row in mime_rows)
            if value is not None
        ]
        formatted.append(
            {
                "mime": mime,
                "similarity_score": _mean(similarity_values),
                "completeness": _mean(completeness_values),
                "unique_word_ratio": _mean(unique_values),
            }
        )
    return formatted


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _rename_single_csv(directory: Path, source_name: str, target_name: str) -> Path | None:
    source = directory / source_name
    target = directory / target_name
    if not source.exists():
        return None
    if target.exists():
        target.unlink()
    source.rename(target)
    return target


def _fmt_float(value: Any) -> str:
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "n/a"
