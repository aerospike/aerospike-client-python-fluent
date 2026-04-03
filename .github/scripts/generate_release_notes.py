#!/usr/bin/env python3
# Copyright 2023-2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Transform GitHub-generated release notes into a custom format:
- Release date at top
- ## Bug Fixes and ## Improvements sections
- JIRA/ticket identifiers (e.g. [CLIENT-1234]) moved to the end of each line (after PR link)
- PR links preserved as ([#N](url))
- Full Changelog link at bottom

Reads raw markdown from stdin; writes formatted markdown to stdout.
"""

import argparse
import re
import sys


TICKET_RE = re.compile(r"\[([A-Z][A-Z0-9]*-[0-9]+)\]")
PR_SUFFIX_RE = re.compile(
    r"\s+by\s+@[\w-]+\s+in\s+(https?://\S+)", re.IGNORECASE
)


def split_description_and_tickets(text: str) -> tuple[str, list[str]]:
    """Strip ticket brackets from text; return (clean description, ticket keys)."""
    tickets = TICKET_RE.findall(text)
    rest = TICKET_RE.sub("", text).strip()
    rest = re.sub(r"  +", " ", rest).strip()
    return rest, tickets


def pr_link_markdown(url: str) -> str:
    m = re.search(r"/pull/(\d+)", url, re.IGNORECASE)
    if m:
        return f"([#{m.group(1)}]({url}))"
    return f"([pull]({url}))"


def is_bug_fix(title: str) -> bool:
    t = title.strip().lower()
    return t.startswith("fix ") or t.startswith("fixes ") or t.startswith("bug") or t.startswith("fix:")


def parse_bullet(line: str) -> tuple[str, str] | None:
    """
    Parse a line like '* Fix something by @user in https://.../pull/99'.

    Returns (formatted_bullet, description_for_bug_vs_improvement) or None.
    """
    line = line.strip()
    if not line.startswith("* ") and not line.startswith("- "):
        return None
    text = line[2:].strip()
    pr_url = ""
    by_match = PR_SUFFIX_RE.search(text)
    if by_match:
        pr_url = by_match.group(1).rstrip(").,;")
        text = text[: by_match.start()].strip()
    if not text:
        return None
    desc, tickets = split_description_and_tickets(text)
    if not desc:
        return None
    parts = [desc]
    if pr_url:
        parts.append(pr_link_markdown(pr_url))
    if tickets:
        parts.append(f"[{tickets[0]}]")
    return " ".join(parts), desc


def main() -> None:
    parser = argparse.ArgumentParser(description="Format release notes.")
    parser.add_argument("--date", required=True, help="Release date (e.g. 'March 17, 2026')")
    parser.add_argument("--changelog-url", default="", help="Full Changelog URL (optional; may be parsed from input)")
    args = parser.parse_args()

    raw = sys.stdin.read()
    bug_fixes: list[str] = []
    improvements: list[str] = []

    changelog_url = args.changelog_url
    for line in raw.splitlines():
        if "Full Changelog" in line or "full changelog" in line.lower():
            url_match = re.search(r"https://[^\s\)]+", line)
            if url_match and not changelog_url:
                changelog_url = url_match.group(0)
            continue
        parsed = parse_bullet(line)
        if parsed is None:
            continue
        formatted, desc_for_category = parsed
        if is_bug_fix(desc_for_category):
            bug_fixes.append(formatted)
        else:
            improvements.append(formatted)

    out = [f"Release Date: {args.date}", ""]
    if bug_fixes:
        out.append("## Bug Fixes")
        for item in bug_fixes:
            out.append(f"- {item}")
        out.append("")
    if improvements:
        out.append("## Improvements")
        for item in improvements:
            out.append(f"- {item}")
        out.append("")
    if changelog_url:
        out.append(f"**Full Changelog**: {changelog_url}")

    sys.stdout.write("\n".join(out))
    if out and not out[-1].endswith("\n"):
        sys.stdout.write("\n")


if __name__ == "__main__":
    main()
