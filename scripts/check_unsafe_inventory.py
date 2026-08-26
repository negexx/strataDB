"""Fail-closed lexical admission check for project-owned Rust unsafe code."""

from __future__ import annotations

import argparse
import dataclasses
import json
from pathlib import Path
import re
import sys


MARKER_RE = re.compile(
    r"^\s*//\s*SAFETY\[([A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*)\]:\s*(\S(?:.*\S)?)\s*$"
)
MARKER_PREFIX_RE = re.compile(r"^\s*//\s*SAFETY\[")
ID_RE = re.compile(r"^[A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*$")


@dataclasses.dataclass(frozen=True)
class UnsafeSite:
    marker: str
    path: str
    line: int
    kind: str
    rationale: str = ""
    marker_error: str = ""


@dataclasses.dataclass(frozen=True)
class InventoryEntry:
    identifier: str
    path: str
    kind: str
    owner: str
    safety: str
    evidence: tuple[str, ...]


def discover_rust_files(root: Path) -> list[Path]:
    """Return every project-owned Rust source beneath *root* deterministically."""
    excluded = {".git", "target", ".worktrees"}
    return sorted(
        (
            path
            for path in root.rglob("*.rs")
            if not any(part in excluded for part in path.relative_to(root).parts)
        ),
        key=lambda path: path.relative_to(root).as_posix(),
    )


def _raw_string_end(source: str, start: int) -> int | None:
    """Return the exclusive end of a raw string beginning at *start*."""
    index = start
    if source.startswith("br", index):
        index += 2
    elif source.startswith("r", index):
        index += 1
    else:
        return None
    hashes = 0
    while index < len(source) and source[index] == "#":
        hashes += 1
        index += 1
    if index >= len(source) or source[index] != '"':
        return None
    terminator = '"' + "#" * hashes
    close = source.find(terminator, index + 1)
    return len(source) if close < 0 else close + len(terminator)


def _char_literal_end(source: str, start: int) -> int | None:
    """Return the end of a Rust character literal, excluding lifetimes/labels."""
    index = start + 1
    if index >= len(source) or source[index] == "\n":
        return None
    if source[index] == "\\":
        index += 1
        if index >= len(source):
            return None
        if source[index] == "x":
            index += 3
        elif source[index] == "u" and index + 1 < len(source) and source[index + 1] == "{":
            close = source.find("}", index + 2)
            if close < 0 or close == index + 2:
                return None
            index = close + 1
        else:
            index += 1
    else:
        index += 1
    return index + 1 if index < len(source) and source[index] == "'" else None


def _mask_non_code(source: str) -> tuple[str, set[int]]:
    """Mask Rust comments and literals while preserving every newline."""
    masked = list(source)
    line_comments: set[int] = set()
    index = 0
    line = 1

    def mask_until(end: int) -> None:
        nonlocal line
        for position in range(index, end):
            if source[position] == "\n":
                line += 1
            else:
                masked[position] = " "

    while index < len(source):
        character = source[index]
        if character == "\n":
            line += 1
            index += 1
            continue
        if source.startswith("//", index):
            line_comments.add(line)
            end = source.find("\n", index)
            if end < 0:
                end = len(source)
            mask_until(end)
            index = end
            continue
        if source.startswith("/*", index):
            depth = 1
            end = index + 2
            while end < len(source) and depth:
                if source.startswith("/*", end):
                    depth += 1
                    end += 2
                elif source.startswith("*/", end):
                    depth -= 1
                    end += 2
                else:
                    end += 1
            mask_until(end)
            index = end
            continue
        raw_end = _raw_string_end(source, index)
        if raw_end is not None:
            mask_until(raw_end)
            index = raw_end
            continue
        if character == '"' or (character == "b" and index + 1 < len(source) and source[index + 1] == '"'):
            end = index + (2 if character == "b" else 1)
            while end < len(source):
                if source[end] == "\\":
                    end += 2
                elif source[end] == '"':
                    end += 1
                    break
                else:
                    end += 1
            mask_until(end)
            index = end
            continue
        if character == "'":
            end = _char_literal_end(source, index)
            if end is not None:
                mask_until(end)
                index = end
                continue
        index += 1
    return "".join(masked), line_comments


def _next_token(masked: str, index: int) -> tuple[str, int]:
    while index < len(masked) and masked[index].isspace():
        index += 1
    if index >= len(masked):
        return "", index
    if masked[index].isalpha() or masked[index] == "_":
        end = index + 1
        while end < len(masked) and (masked[end].isalnum() or masked[end] == "_"):
            end += 1
        return masked[index:end], end
    return masked[index], index + 1


def _marker_before(
    lines: list[str], line_comments: set[int], construct_line: int
) -> tuple[str, str, str]:
    """Return marker, rationale, and a policy error for a nearby marker paragraph."""
    candidates: list[tuple[str, str]] = []
    malformed = False
    line_index = construct_line - 2
    while line_index >= 0:
        stripped = lines[line_index].strip()
        line_number = line_index + 1
        if not stripped:
            line_index -= 1
            continue
        if line_number in line_comments and stripped.startswith("//"):
            match = MARKER_RE.match(lines[line_index])
            if match:
                candidates.append((match.group(1), match.group(2)))
            elif MARKER_PREFIX_RE.match(lines[line_index]):
                malformed = True
            line_index -= 1
            continue
        if stripped.startswith("#[") or stripped == "]":
            line_index -= 1
            continue
        break
    if malformed:
        return "", "", "malformed SAFETY marker"
    if not candidates:
        return "", "", ""
    if len(candidates) != 1:
        return "", "", "multiple SAFETY markers apply to one unsafe construct"
    return candidates[0][0], candidates[0][1], ""


def _markers_in_source(lines: list[str], line_comments: set[int]) -> list[tuple[str, int]]:
    markers: list[tuple[str, int]] = []
    for line_number, line in enumerate(lines, start=1):
        if line_number not in line_comments:
            continue
        match = MARKER_RE.match(line)
        if match:
            markers.append((match.group(1), line_number))
    return markers


def scan_rust_source(path: str, source: str) -> list[UnsafeSite]:
    """Classify every executable `unsafe` construct in one Rust source file."""
    masked, line_comments = _mask_non_code(source)
    lines = source.splitlines()
    sites: list[UnsafeSite] = []
    for match in re.finditer(r"(?<![A-Za-z0-9_])unsafe(?![A-Za-z0-9_])", masked):
        token, _ = _next_token(masked, match.end())
        kind = {"{": "block", "fn": "function", "impl": "impl"}.get(token, "unknown")
        line = masked.count("\n", 0, match.start()) + 1
        marker, rationale, marker_error = _marker_before(lines, line_comments, line)
        sites.append(UnsafeSite(marker, path, line, kind, rationale, marker_error))
    return sites


def load_inventory(path: Path) -> dict[str, InventoryEntry]:
    """Load schema-version-one inventory data, rejecting malformed inputs."""
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ValueError("inventory must be a schema_version 1 object")
    raw_sites = document.get("sites")
    if not isinstance(raw_sites, list):
        raise ValueError("inventory sites must be an array")
    entries: dict[str, InventoryEntry] = {}
    for index, raw in enumerate(raw_sites):
        if not isinstance(raw, dict):
            raise ValueError(f"inventory site {index} must be an object")
        identifier = raw.get("id")
        if not isinstance(identifier, str):
            raise ValueError(f"inventory site {index} id must be a string")
        if identifier in entries:
            raise ValueError(f"duplicate inventory id {identifier}")
        evidence = raw.get("evidence")
        entries[identifier] = InventoryEntry(
            identifier=identifier,
            path=raw.get("path"),
            kind=raw.get("kind"),
            owner=raw.get("owner"),
            safety=raw.get("safety"),
            evidence=tuple(evidence) if isinstance(evidence, list) else (),
        )
    return entries


def _entry_errors(entry: InventoryEntry) -> list[str]:
    errors: list[str] = []
    if not ID_RE.fullmatch(entry.identifier):
        errors.append("id must use uppercase ASCII kebab case")
    if not isinstance(entry.path, str) or not entry.path or "\\" in entry.path or entry.path.startswith("/") or ".." in Path(entry.path).parts:
        errors.append("path must be a slash-normalized relative path without ..")
    if entry.kind not in {"block", "function", "impl"}:
        errors.append("kind must be block, function, or impl")
    for field_name, value in (("owner", entry.owner), ("safety", entry.safety)):
        if not isinstance(value, str) or not value.strip():
            errors.append(f"{field_name} must be a non-empty string")
    if not entry.evidence or any(not isinstance(value, str) or not value.strip() for value in entry.evidence):
        errors.append("evidence must be a non-empty array of non-empty strings")
    if entry.kind == "block" and any(value.startswith("review-only:") for value in entry.evidence):
        errors.append("block evidence must be executable, not review-only")
    return errors


def validate_inventory(root: Path, inventory_path: Path) -> list[str]:
    """Return sorted policy diagnostics; malformed inputs raise for CLI exit 2."""
    root = root.resolve()
    inventory_path = inventory_path.resolve()
    entries = load_inventory(inventory_path)
    errors: list[str] = []
    sites: list[UnsafeSite] = []
    marker_locations: dict[str, list[tuple[str, int]]] = {}
    for rust_file in discover_rust_files(root):
        relative_path = rust_file.relative_to(root).as_posix()
        source = rust_file.read_text(encoding="utf-8")
        masked, line_comments = _mask_non_code(source)
        lines = source.splitlines()
        for marker, line in _markers_in_source(lines, line_comments):
            marker_locations.setdefault(marker, []).append((relative_path, line))
        sites.extend(scan_rust_source(relative_path, source))

    discovered_markers: set[str] = set()
    for site in sites:
        prefix = f"{site.path}:{site.line}:"
        if site.kind == "unknown":
            errors.append(f"{prefix} unknown unsafe form")
            continue
        if site.marker_error:
            errors.append(f"{prefix} {site.marker_error}")
            continue
        if not site.marker:
            errors.append(f"{prefix} unmarked unsafe {site.kind}")
            continue
        discovered_markers.add(site.marker)
        locations = marker_locations.get(site.marker, [])
        if len(locations) != 1:
            errors.append(f"{prefix} duplicate marker {site.marker}")
        entry = entries.get(site.marker)
        if entry is None:
            errors.append(f"{prefix} marker has no inventory entry: {site.marker}")
            continue
        if entry.path != site.path:
            errors.append(f"{prefix} path mismatch for {site.marker}: inventory has {entry.path}")
        if entry.kind != site.kind:
            errors.append(f"{prefix} kind mismatch for {site.marker}: inventory has {entry.kind}")
        if entry.safety != site.rationale:
            errors.append(f"{prefix} safety rationale mismatch for {site.marker}")

    for marker, locations in marker_locations.items():
        if marker not in discovered_markers:
            path, line = locations[0]
            errors.append(f"{path}:{line}: marker does not apply to an unsafe construct: {marker}")
    for identifier, entry in entries.items():
        entry_prefix = f"{entry.path if isinstance(entry.path, str) else '<invalid>'}:0:"
        for reason in _entry_errors(entry):
            errors.append(f"{entry_prefix} {reason}")
        if identifier not in discovered_markers:
            errors.append(f"{entry_prefix} stale inventory entry: {identifier}")
    return sorted(set(errors))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--inventory", required=True, type=Path)
    arguments = parser.parse_args(argv)
    try:
        errors = validate_inventory(arguments.root, arguments.inventory)
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as error:
        print(f"input error: {error}", file=sys.stderr)
        return 2
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    sites = [
        site
        for path in discover_rust_files(arguments.root.resolve())
        for site in scan_rust_source(
            path.relative_to(arguments.root.resolve()).as_posix(), path.read_text(encoding="utf-8")
        )
    ]
    blocks = sum(site.kind == "block" for site in sites)
    declarations = sum(site.kind in {"function", "impl"} for site in sites)
    print(
        f"{len(sites)} approved unsafe constructs ({blocks} blocks, "
        f"{declarations} functions/impls) across {len({site.path for site in sites})} files"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
