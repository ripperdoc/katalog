from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from katalog.constants.metadata import MetadataKey, get_metadata_def_by_key


class TemplateError(ValueError):
    """Base class for path template parsing/evaluation errors."""


class TemplateParseError(TemplateError):
    """Raised when a template cannot be parsed or validated."""


class TemplateEvaluationError(TemplateError):
    """Raised when a parsed template cannot be evaluated."""


class TemplateValueMissingError(TemplateEvaluationError):
    """Raised when a placeholder resolves to no value."""


_ALLOWED_SPECIFIERS = frozenset({"latest", "year", "month"})


@dataclass(frozen=True)
class TemplateExpr:
    key: MetadataKey | None = None
    specifiers: tuple[str, ...] = ()


@dataclass(frozen=True)
class TemplatePart:
    literal: str | None = None
    expressions: tuple[TemplateExpr, ...] = ()

    def is_literal(self) -> bool:
        return self.literal is not None


@dataclass(frozen=True)
class CompiledTemplate:
    template: str
    parts: tuple[TemplatePart, ...]
    keys: frozenset[MetadataKey]


def normalize_template_key(raw_key: str) -> MetadataKey:
    key = raw_key.strip()
    if not key:
        raise TemplateParseError("Placeholder key cannot be empty")
    # Support both file/path and file.path forms.
    normalized = key.replace(".", "/")
    return MetadataKey(normalized)


def compile_template(
    template: str,
    *,
    validate_metadata_keys: bool = True,
) -> CompiledTemplate:
    if not template:
        raise TemplateParseError("Template cannot be empty")

    parts: list[TemplatePart] = []
    keys: set[MetadataKey] = set()
    literal_buf: list[str] = []
    idx = 0
    length = len(template)

    while idx < length:
        char = template[idx]
        if char == "{":
            if idx + 1 < length and template[idx + 1] == "{":
                literal_buf.append("{")
                idx += 2
                continue
            if literal_buf:
                parts.append(TemplatePart(literal="".join(literal_buf)))
                literal_buf = []

            end = template.find("}", idx + 1)
            if end < 0:
                raise TemplateParseError("Unclosed '{' in template")
            expr = template[idx + 1 : end].strip()
            if not expr:
                raise TemplateParseError("Empty placeholder '{}' is not allowed")
            expressions = _parse_placeholder_expressions(expr)
            for placeholder_expr in expressions:
                if placeholder_expr.key is None:
                    continue
                keys.add(placeholder_expr.key)
                if validate_metadata_keys:
                    _validate_metadata_key(placeholder_expr.key)
            parts.append(TemplatePart(expressions=expressions))
            idx = end + 1
            continue
        if char == "}":
            if idx + 1 < length and template[idx + 1] == "}":
                literal_buf.append("}")
                idx += 2
                continue
            raise TemplateParseError("Unmatched '}' in template")
        literal_buf.append(char)
        idx += 1

    if literal_buf:
        parts.append(TemplatePart(literal="".join(literal_buf)))

    if not parts:
        raise TemplateParseError("Template cannot be empty")

    return CompiledTemplate(
        template=template,
        parts=tuple(parts),
        keys=frozenset(keys),
    )


def evaluate_template(
    compiled: CompiledTemplate,
    *,
    resolver: Callable[[MetadataKey], object],
) -> str:
    rendered: list[str] = []
    for part in compiled.parts:
        if part.is_literal():
            rendered.append(part.literal or "")
            continue
        rendered.append(_evaluate_placeholder(part, resolver=resolver))
    return "".join(rendered)


def _parse_placeholder_expressions(expr: str) -> tuple[TemplateExpr, ...]:
    branches = [item.strip() for item in expr.split("|")]
    if any(not branch for branch in branches):
        raise TemplateParseError(f"Invalid fallback expression: {expr!r}")
    expressions = tuple(_parse_expression(branch) for branch in branches)
    if not expressions:
        raise TemplateParseError(f"Invalid placeholder expression: {expr!r}")
    return expressions


def _parse_expression(expr: str) -> TemplateExpr:
    raw_parts = [item.strip() for item in expr.split(":")]
    if not raw_parts or not raw_parts[0]:
        raise TemplateParseError(f"Invalid placeholder expression: {expr!r}")

    key = normalize_template_key(raw_parts[0])
    raw_specifiers = [item.lower() for item in raw_parts[1:] if item]
    if len(raw_parts) > 1 and len(raw_specifiers) != len(raw_parts) - 1:
        raise TemplateParseError(f"Empty specifier in placeholder: {expr!r}")

    if not raw_specifiers:
        specifiers = ["latest"]
    elif raw_specifiers[0] != "latest":
        specifiers = ["latest", *raw_specifiers]
    else:
        specifiers = list(raw_specifiers)

    for specifier in specifiers:
        if specifier not in _ALLOWED_SPECIFIERS:
            raise TemplateParseError(
                f"Unknown template specifier {specifier!r} in {expr!r}"
            )

    return TemplateExpr(key=key, specifiers=tuple(specifiers))


def _validate_metadata_key(key: MetadataKey) -> None:
    try:
        get_metadata_def_by_key(key)
    except Exception as exc:
        raise TemplateParseError(f"Unknown metadata key {key!s}") from exc


def _apply_specifier(specifier: str, value: object, *, key: MetadataKey) -> object:
    if specifier == "latest":
        return _apply_latest(value, key=key)
    if specifier == "year":
        dt = _coerce_to_datetime(value, key=key, specifier=specifier)
        return f"{dt.year:04d}"
    if specifier == "month":
        dt = _coerce_to_datetime(value, key=key, specifier=specifier)
        return f"{dt.month:02d}"
    raise TemplateEvaluationError(
        f"Unknown template specifier {specifier!r} for key {key!s}"
    )


def _evaluate_placeholder(
    part: TemplatePart, *, resolver: Callable[[MetadataKey], object]
) -> str:
    if part.literal is not None:
        return part.literal
    if not part.expressions:
        raise TemplateEvaluationError("Malformed compiled template part")

    missing_errors: list[TemplateValueMissingError] = []
    for expression in part.expressions:
        key = expression.key
        if key is None:
            raise TemplateEvaluationError("Malformed placeholder expression")
        try:
            value = resolver(key)
            for specifier in expression.specifiers:
                value = _apply_specifier(specifier, value, key=key)
            if value is None:
                raise TemplateValueMissingError(
                    f"Missing value for metadata key {key!s}"
                )
            text = _coerce_to_text(value, key=key)
            if text != "":
                return text
        except TemplateValueMissingError as exc:
            missing_errors.append(exc)
            continue

    if missing_errors:
        raise missing_errors[-1]
    raise TemplateValueMissingError("All fallback expressions resolved to empty values")


def _apply_latest(value: object, *, key: MetadataKey) -> object:
    if value is None:
        raise TemplateValueMissingError(f"Missing value for metadata key {key!s}")
    if isinstance(value, (list, tuple)):
        if not value:
            raise TemplateValueMissingError(f"Missing value for metadata key {key!s}")
        chosen = value[0]
    else:
        chosen = value
    if hasattr(chosen, "value"):
        chosen = getattr(chosen, "value")
    if chosen is None:
        raise TemplateValueMissingError(f"Missing value for metadata key {key!s}")
    return chosen


def _coerce_to_datetime(value: object, *, key: MetadataKey, specifier: str) -> datetime:
    raw = value
    if hasattr(raw, "value"):
        raw = getattr(raw, "value")
    if not isinstance(raw, datetime):
        raise TemplateEvaluationError(
            f"Specifier {specifier!r} requires datetime value for key {key!s}"
        )
    return raw


def _coerce_to_text(value: object, *, key: MetadataKey) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    raise TemplateEvaluationError(
        f"Template key {key!s} resolved to unsupported value type {type(value)!r}"
    )
