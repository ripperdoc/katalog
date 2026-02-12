from __future__ import annotations

from datetime import UTC, datetime

import pytest

from katalog.constants.metadata import FILE_PATH, TIME_CREATED, TIME_MODIFIED
from katalog.processors.path_template import (
    TemplateParseError,
    TemplateValueMissingError,
    compile_template,
    evaluate_template,
)


def test_compile_template_normalizes_dotted_key_and_defaults_latest():
    compiled = compile_template("{file.path}")

    part = compiled.parts[0]
    expr = part.expressions[0]
    assert expr.key == FILE_PATH
    assert expr.specifiers == ("latest",)
    assert FILE_PATH in compiled.keys


def test_compile_template_inserts_latest_before_transform():
    compiled = compile_template("{time.modified:year}")

    part = compiled.parts[0]
    expr = part.expressions[0]
    assert expr.key == TIME_MODIFIED
    assert expr.specifiers == ("latest", "year")


def test_compile_template_unknown_key_raises():
    with pytest.raises(TemplateParseError, match="Unknown metadata key"):
        compile_template("{missing.key}")


def test_compile_template_invalid_fallback_expression_raises():
    with pytest.raises(TemplateParseError, match="Invalid fallback expression"):
        compile_template("{time.modified:latest|}")


def test_compile_template_collects_all_dependency_keys_across_fallbacks():
    compiled = compile_template("{time.modified:latest|time.created:latest}")

    assert compiled.keys == frozenset({TIME_MODIFIED, TIME_CREATED})


def test_evaluate_template_latest_and_literal_text():
    compiled = compile_template("prefix/{file/path}/suffix")

    def resolver(key):
        if key == FILE_PATH:
            return ["/a/newer", "/a/older"]
        return []

    rendered = evaluate_template(compiled, resolver=resolver)
    assert rendered == "prefix//a/newer/suffix"


def test_evaluate_template_datetime_year_and_month():
    compiled = compile_template("{time.modified:year}/{time.created:month}")
    modified = datetime(2024, 6, 3, 8, 1, tzinfo=UTC)
    created = datetime(2019, 2, 28, 20, 15, tzinfo=UTC)

    def resolver(key):
        if key == TIME_MODIFIED:
            return [modified]
        if key == TIME_CREATED:
            return [created]
        return []

    rendered = evaluate_template(compiled, resolver=resolver)
    assert rendered == "2024/02"


def test_evaluate_template_fallback_uses_first_non_empty_branch():
    compiled = compile_template("{time.modified:latest|time.created:latest}")
    created = datetime(2021, 4, 1, 0, 0, tzinfo=UTC)

    def resolver(key):
        if key == TIME_MODIFIED:
            return []
        if key == TIME_CREATED:
            return [created]
        return []

    rendered = evaluate_template(compiled, resolver=resolver)
    assert rendered == created.isoformat()


def test_evaluate_template_fallback_missing_raises():
    compiled = compile_template("{time.modified:latest|time.created:latest}")

    def resolver(_key):
        return []

    with pytest.raises(TemplateValueMissingError, match="Missing value"):
        evaluate_template(compiled, resolver=resolver)
