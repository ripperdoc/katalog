import argparse
import csv
import json
from pathlib import Path
from typing import Dict, Iterable, Tuple

from markdown import markdown as markdown_to_html
from markdownify import (
    MarkdownConverter,
    abstract_inline_conversion,
    markdownify as html_to_markdown,
)
from openai import OpenAI

LANGUAGE_FIELDS: Dict[str, Tuple[str, str, str]] = {
    "swedish": ("Titel (SV)", "Inledande text (SV)", "Full text (SV)"),
    "english": ("Titel (EN)", "Inledande text (EN)", "Full text (EN)"),
}
SEPARATOR = "\n\n---\n\n"
DEFAULT_ENCODING = "utf-8"


def normalize_language(label: str) -> str:
    normalized = label.strip().lower()
    if normalized in {"sv", "svenska"}:
        return "swedish"
    if normalized in {"en", "eng"}:
        return "english"
    return normalized


def convert_html_to_markdown(value: str) -> str:
    if not value:
        return ""
    converter = EmphasisConverter(heading_style="ATX")
    return converter.convert(value).strip()


def convert_markdown_to_html(value: str) -> str:
    if not value:
        return ""
    # Using the markdown library keeps the CSV round-trippable from markdown files.
    return markdown_to_html(value.strip(), extensions=["extra"])


def build_markdown_document(title: str, initial: str, full: str) -> str:
    title_part = title.strip()
    initial_part = initial.strip()
    full_part = full.strip()
    return f"{title_part}{SEPARATOR}{initial_part}{SEPARATOR}{full_part}\n"


def parse_markdown_document(content: str) -> Tuple[str, str, str]:
    if SEPARATOR not in content:
        raise ValueError("Expected two '---' separators to split initial and full text")

    title_part, remainder = content.split(SEPARATOR, 1)
    if SEPARATOR not in remainder:
        raise ValueError("Expected final separator after initial section")

    initial_part, full_part = remainder.split(SEPARATOR, 1)
    return title_part.strip(), initial_part.strip(), full_part.strip()


def export_csv_to_markdown(csv_path: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    with csv_path.open("r", encoding=DEFAULT_ENCODING, newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            article = row.get("Article", "").strip()
            if not article:
                continue

            for lang_key, fields in LANGUAGE_FIELDS.items():
                title_html, initial_html, full_html = (row.get(field, "") for field in fields)
                if not any([title_html, initial_html, full_html]):
                    continue

                title_md = convert_html_to_markdown(title_html)
                initial_md = convert_html_to_markdown(initial_html)
                full_md = convert_html_to_markdown(full_html)

                lang_dir = output_dir / lang_key
                lang_dir.mkdir(parents=True, exist_ok=True)
                file_path = lang_dir / f"{article}.md"
                document = build_markdown_document(title_md, initial_md, full_md)
                file_path.write_text(document, encoding=DEFAULT_ENCODING)


def read_markdown_files(root: Path) -> Dict[str, Dict[str, Tuple[str, str, str]]]:
    files: Dict[str, Dict[str, Tuple[str, str, str]]] = {}
    for lang_dir in root.iterdir():
        if not lang_dir.is_dir():
            continue
        lang_key = normalize_language(lang_dir.name)
        if lang_key not in LANGUAGE_FIELDS:
            continue

        for file_path in lang_dir.glob("*.md"):
            content = file_path.read_text(encoding=DEFAULT_ENCODING)
            try:
                title, initial_md, full_md = parse_markdown_document(content)
            except ValueError as err:
                raise ValueError(f"{file_path}: {err}") from err

            article = file_path.stem
            files.setdefault(article, {})[lang_key] = (title, initial_md, full_md)
    return files


def compile_csv_from_markdown(input_dir: Path, csv_path: Path) -> None:
    entries = read_markdown_files(input_dir)
    fieldnames = [
        "Article",
        "Language",
        "Titel (SV)",
        "Inledande text (SV)",
        "Full text (SV)",
        "Titel (EN)",
        "Inledande text (EN)",
        "Full text (EN)",
    ]

    with csv_path.open("w", encoding=DEFAULT_ENCODING, newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for article, languages in sorted(entries.items()):
            row = {field: "" for field in fieldnames}
            row["Article"] = article

            if "swedish" in languages:
                title, initial_md, full_md = languages["swedish"]
                row["Language"] = "Swedish"
                row["Titel (SV)"] = convert_markdown_to_html(title)
                row["Inledande text (SV)"] = convert_markdown_to_html(initial_md)
                row["Full text (SV)"] = convert_markdown_to_html(full_md)

            if "english" in languages:
                title, initial_md, full_md = languages["english"]
                row["Language"] = row["Language"] or "English"
                row["Titel (EN)"] = convert_markdown_to_html(title)
                row["Inledande text (EN)"] = convert_markdown_to_html(initial_md)
                row["Full text (EN)"] = convert_markdown_to_html(full_md)

            writer.writerow(row)


def clamp_initial_text(value: str, limit: int = 350) -> str:
    text = value.strip()
    if len(text) <= limit:
        return text
    return text[:limit].rstrip()


def suggest_updates(
    client: OpenAI, model: str, title: str, initial: str, full: str
) -> Tuple[str, str]:
    messages = [
        {
            "role": "system",
            "content": (
                "You rewrite product titles and short descriptions without changing meaning. "
                "Keep the language the same as input, and keep the short description within 350 characters. "
                "Return JSON with keys 'title' and 'initial'."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Title:\n{title}\n\n"
                f"Initial description:\n{initial or '(missing)'}\n\n"
                f"Full description:\n{full}\n\n"
                "Return JSON with fields 'title' and 'initial' only."
            ),
        },
    ]

    completion = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.2,
        response_format={"type": "json_object"},
    )
    content = completion.choices[0].message.content
    try:
        payload = json.loads(content or "{}")
    except json.JSONDecodeError as err:
        raise ValueError(f"Could not parse model response: {content}") from err

    new_title = payload.get("title", title).strip() or title
    new_initial = payload.get("initial", initial).strip() or initial
    return new_title, clamp_initial_text(new_initial)


def refine_markdown_files(input_dir: Path, model: str, api_key: str | None) -> None:
    client = OpenAI(api_key=api_key)
    for lang_dir in input_dir.iterdir():
        if not lang_dir.is_dir():
            continue
        lang_key = normalize_language(lang_dir.name)
        if lang_key not in LANGUAGE_FIELDS:
            continue

        for file_path in lang_dir.glob("*.md"):
            content = file_path.read_text(encoding=DEFAULT_ENCODING)
            title, initial_md, full_md = parse_markdown_document(content)
            new_title, new_initial = suggest_updates(
                client=client, model=model, title=title, initial=initial_md, full=full_md
            )
            updated = build_markdown_document(new_title, new_initial, full_md)
            file_path.write_text(updated, encoding=DEFAULT_ENCODING)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync product descriptions between CSV and per-language markdown files."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser(
        "export", help="Convert CSV product texts into markdown files."
    )
    export_parser.add_argument(
        "--csv",
        dest="csv_path",
        type=Path,
        default=Path("tools") / "Product List - Black Week Sales - Sheet12.csv",
        help="Path to the source CSV file.",
    )
    export_parser.add_argument(
        "--output",
        dest="output_dir",
        type=Path,
        default=Path("tools") / "prodtext",
        help="Directory where markdown files will be written.",
    )

    import_parser = subparsers.add_parser(
        "import", help="Rebuild the CSV file from markdown product texts."
    )
    import_parser.add_argument(
        "--input",
        dest="input_dir",
        type=Path,
        default=Path("tools") / "prodtext",
        help="Directory containing per-language markdown files.",
    )
    import_parser.add_argument(
        "--csv",
        dest="csv_path",
        type=Path,
        default=Path("tools") / "Product List - Black Week Sales - Sheet12.csv",
        help="Path where the rebuilt CSV will be written.",
    )

    refine_parser = subparsers.add_parser(
        "refine",
        help="Call OpenAI to suggest improved titles and initial descriptions in markdown files.",
    )
    refine_parser.add_argument(
        "--input",
        dest="input_dir",
        type=Path,
        default=Path("tools") / "prodtext",
        help="Directory containing per-language markdown files.",
    )
    refine_parser.add_argument(
        "--model",
        dest="model",
        type=str,
        default="gpt-4o-mini",
        help="OpenAI model to use.",
    )
    refine_parser.add_argument(
        "--api-key",
        dest="api_key",
        type=str,
        default=None,
        help="OpenAI API key (defaults to environment).",
    )

    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_argument_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == "export":
        export_csv_to_markdown(args.csv_path, args.output_dir)
    elif args.command == "import":
        compile_csv_from_markdown(args.input_dir, args.csv_path)
    elif args.command == "refine":
        refine_markdown_files(args.input_dir, args.model, args.api_key)
    else:
        parser.error("No command provided.")


class EmphasisConverter(MarkdownConverter):
    # Use underscores for italics and asterisks for bold.
    def convert_em(self, el, text, parent_tags):
        return abstract_inline_conversion(lambda _self: "_")(self, el, text, parent_tags)

    convert_i = convert_em

    def convert_b(self, el, text, parent_tags):
        return abstract_inline_conversion(lambda _self: "**")(self, el, text, parent_tags)

    convert_strong = convert_b


if __name__ == "__main__":
    main()
