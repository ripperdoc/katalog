def _fts5_query_from_user_text(raw: str) -> str:
    """Convert arbitrary user input into a safe FTS5 MATCH query.

    We intentionally do not expose FTS query syntax to the UI search box.
    Characters like '-' can be parsed as operators and crash the query.

    Important: our FTS table is created with `detail='none'` for minimal index
    size, which means FTS5 phrase queries (double-quoted terms) are not
    supported.

    We therefore create an AND query of *tokens* only.

    Example:
    input:  "foo-bar baz" -> "foo AND bar AND baz"
    """

    text = (raw or "").strip()
    if not text:
        return ""

    # Extract only alphanumeric runs; everything else is treated as a separator.
    # This intentionally splits on '_' as well, because the unicode61 tokenizer
    # may treat it as a separator. If we pass a token containing '_' through to
    # FTS5, it may be internally split into multiple adjacent tokens, which then
    # becomes a phrase query (unsupported with detail='none').
    cleaned = text.replace('"', " ")
    parts: list[str] = []
    buf: list[str] = []
    for ch in cleaned:
        if ch.isalnum():
            buf.append(ch)
        else:
            if buf:
                parts.append("".join(buf))
                buf.clear()
    if buf:
        parts.append("".join(buf))
    if not parts:
        return ""

    # Prevent accidental operator injection / parse errors for reserved keywords.
    # Appending '*' makes it a term token (prefix query), including the exact
    # keyword itself, without needing phrase quotes.
    reserved = {"and", "or", "not", "near"}
    safe_parts: list[str] = []
    for part in parts:
        lowered = part.lower()
        safe_parts.append(f"{part}*" if lowered in reserved else part)

    return " AND ".join(safe_parts)
