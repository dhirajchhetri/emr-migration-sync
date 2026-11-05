"""Keep operational docs free of notes that do not belong in the project."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP = {".git", ".venv", "cdk.out", "__pycache__", ".local", ".pytest_cache", ".ruff_cache"}
SUFFIXES = {
    ".py",
    ".md",
    ".html",
    ".yml",
    ".yaml",
    ".json",
    ".toml",
    ".sh",
    ".hl7",
    ".xml",
    ".csv",
    ".jsonl",
}
FORBIDDEN = re.compile(
    "|".join(
        [
            "eazyml",
            "proposed design",
            "co-authored-by",
            "interview prep",
            r"\bllm\b",
            r"\bcursor\b",
            r"\bassistant\b",
            r"\bagents?\b",
        ]
    ),
    re.IGNORECASE,
)


def test_tracked_text_stays_in_project_language():
    problems: list[str] = []
    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue
        if any(part in SKIP for part in path.parts):
            continue
        if path.suffix.lower() not in SUFFIXES:
            continue
        if path.name == "test_language.py":
            continue
        text = path.read_text(errors="ignore")
        if FORBIDDEN.search(text) or re.search(r"\bAI\b", text):
            problems.append(str(path.relative_to(ROOT)))
    assert problems == []
