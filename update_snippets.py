#!/usr/bin/env python3
import os
import re
import sys
import difflib
from pathlib import Path
"""
Finds code snippets and syncs them with the source files.

Only the code snippets that are marked with markers in the comments are synced.
For example:
<!-- INSERT path:./.env.example text_id:dataflow_region lang:bash -->
<!-- END -->

It is also necessary to add markers in the source file.
# [START dataflow_region]
# [END]
"""
ROOT_DIR = Path(__file__).parent
GUIDE_FILE = ROOT_DIR / "GUIDE.md"

guide_content = GUIDE_FILE.read_text()
MATCHER_PATTERN = re.compile(
    r"(?P<START_MARKER><!-- INSERT path:(?P<FILENAME>[./_a-zA-Z0-9]+) text_id:(?P<TEXT_ID>[.a-zA-Z0-9_]+)(?: lang:(?P<LANG>[a-zA-Z0-9]+))? ?-->)"
    r".+?"
    r"(?P<END_MARKER><!-- END -->)",
    re.DOTALL,
)

new_content = guide_content
for match in reversed(list(MATCHER_PATTERN.finditer(guide_content))):
    filename = match.group("FILENAME")
    text_id = match.group("TEXT_ID")
    lang = match.group("LANG") or ""
    start_marker = f"# [START {text_id}]\n"
    end_marker = "# [END]"
    target_filepath = ROOT_DIR / filename
    print(f"Found snippet: filename={filename}, text_id={text_id} lang={lang}")
    target_content = target_filepath.read_text()
    start_pos = target_content.find(start_marker)
    if start_pos == -1:
        raise Exception(
            f"Unable to locate {start_marker!r} in file {target_filepath!r}"
        )
    end_pos = target_content.find(end_marker, start_pos)
    if end_pos == -1:
        raise Exception(
            f"Unable to locate {start_marker!r} in file {target_filepath!r} after pos {start_pos}"
        )
    part_text = target_content[start_pos + len(start_marker) : end_pos - 1]
    new_content = (
        f"{new_content[:match.start()]}{match.group('START_MARKER')}\n"
        f"```{lang}\n"
        f"{part_text}\n"
        f"```\n"
        f"{match.group('END_MARKER')}{new_content[match.end():]}"
    )


class bcolors:
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    RED = "\033[91m"
    ENDC = "\033[0m"


def color_diff(diff):
    for line in diff:
        if line.startswith("+"):
            yield bcolors.GREEN + line + bcolors.ENDC
        elif line.startswith("-"):
            yield bcolors.RED + line + bcolors.ENDC
        elif line.startswith("^"):
            yield bcolors.BLUE + line + bcolors.ENDC
        else:
            yield line


def prepare_diff(left, right):
    return "".join(
        color_diff(
            difflib.unified_diff(
                left.splitlines(keepends=True), right.splitlines(keepends=True)
            )
        )
    )


if new_content != guide_content:
    ff = prepare_diff(guide_content, new_content)
    print("".join(ff))
    if not os.isatty(sys.stdout.fileno()):
        print("File mismatch")
        print("Expected content:")
        print(new_content)
        sys.exit(1)
    selection = input("The content mismatch. Are you want to update? [Y/n]")
    if selection == "" or selection.lower() == "y":
        GUIDE_FILE.write_text(new_content)
        print("File updated")
    else:
        print("Operation aborted")
        sys.exit(1)
else:
    print("File corrects! Snippets no needs updates.")
