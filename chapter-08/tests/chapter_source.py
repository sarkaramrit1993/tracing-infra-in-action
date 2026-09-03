#!/usr/bin/env python3
"""Read a printed listing out of the book's AsciiDoc source.

The tests use this to compare a shipped `.sql` file against the chapter it was
printed in, rather than against a copy of the chapter pasted into the test file.
A pasted copy only ever proves that two files in this repository agree with each
other, which is exactly what was true while chapter 7, and later chapter 8, sat
green on listings that had drifted off the page.

The manuscript is a separate repository. A reader who clones this one does not
have it, so nothing here raises when the chapter is missing: `find_chapters`
returns an empty list and the caller skips.

The search is a convenience for the one-checkout case and nothing more. A
machine that has done any real work on the book holds several checkouts of it,
most of them parked on a branch for some other chapter, and no rule over
directory names or timestamps reliably picks the one being edited out of them.
So when more than one turns up the caller is expected to fail and ask, and the
way to answer is TRACING_MANUSCRIPT, or the untracked pointer file beside this
one, either of which may name a checkout or the chapter file itself.
"""
import os
import re
from pathlib import Path

ENV_VAR = "TRACING_MANUSCRIPT"

# An untracked file holding one path, for a machine that has the book and would
# otherwise be typing the variable above on every run. The variable wins over it.
POINTER = Path(__file__).resolve().parent / "manuscript.path"

# Where a chapter sits inside the manuscript repository. A chapter moves from
# the first of these to the second on handover, so both are searched.
#
# The starred pair is the same two, one directory further down. A checkout is as
# often a worktree parked inside a directory of worktrees as it is a sibling of
# this repository, and searching only the sibling level is how the comparison
# came to report nothing to compare while the chapter sat two levels away.
_CHAPTER_GLOBS = (
    "manuscript/chapters in progress/CH{:02d}_*.adoc",
    "manuscript/CH{:02d}_*.adoc",
    "*/manuscript/chapters in progress/CH{:02d}_*.adoc",
    "*/manuscript/CH{:02d}_*.adoc",
)

# A callout marker at the end of a line is AsciiDoc, not SQL: " <1>" is what
# ties the line to the numbered note printed under the listing.
_CALLOUT = re.compile(r"\s+<\d+>$")


def configured_location():
    """Where this machine says the book is, or None if it has not said.

    TRACING_MANUSCRIPT first, then the pointer file. Either may name a checkout
    or a single chapter file. Neither is needed to run the rest of the suite.
    """
    setting = os.environ.get(ENV_VAR)
    if not setting and POINTER.exists():
        setting = POINTER.read_text().strip()
    return Path(setting).expanduser() if setting else None


def find_chapters(number):
    """Every AsciiDoc file for a chapter that this machine has.

    Empty when the manuscript is not here, which is the ordinary case for anyone
    who cloned the code and not the book. More than one when several checkouts
    sit side by side, and the caller has to say which: quietly picking one is how
    a comparison ends up passing against a copy nobody is editing.
    """
    chosen = configured_location()
    if chosen is not None:
        if chosen.is_file():
            return [chosen.resolve()]
        roots = [chosen]
    else:
        repo = Path(__file__).resolve().parents[2]
        roots = [repo] + sorted(p for p in repo.parent.iterdir() if p.is_dir())
    found = []
    for root in roots:
        for glob in _CHAPTER_GLOBS:
            for path in sorted(root.glob(glob.format(number))):
                resolved = path.resolve()
                if resolved not in found:
                    found.append(resolved)
    return found


def listing_sql(chapter, anchor):
    """The SQL inside one listing block, with the callout markers taken off.

    A listing is written like this, and the anchor is the id in the first line:

        [#ch8-listing-1, reftext=...]
        .Biased and unbiased aggregates over sampled data
        [source,sql]
        ----
        SELECT service_name, count() AS requests <1>
        ----
        <1> what the first callout says
    """
    lines = chapter.read_text().splitlines()
    start = _index(lines, lambda line: line.startswith(("[#%s," % anchor,
                                                        "[#%s]" % anchor)))
    if start is None:
        raise LookupError("%s has no listing anchored %s" % (chapter.name, anchor))
    source = _index(lines, lambda line: line.startswith("[source,"), start)
    if source is None:
        raise LookupError("%s has no source block" % anchor)
    opened = _index(lines, lambda line: line.rstrip() == "----", source)
    closed = None if opened is None else _index(
        lines, lambda line: line.rstrip() == "----", opened + 1)
    if closed is None:
        raise LookupError("%s is not fenced" % anchor)
    return _trim(_CALLOUT.sub("", line.rstrip())
                 for line in lines[opened + 1:closed])


def shipped_sql(path, number):
    """The region of a companion `.sql` file that reproduces a printed listing.

    The file marks it with comments, because the file also carries setup a
    reader needs and the page does not print:

        -- ---- Listing 8.2: Add a bloom-filter skip index ----
        ...
        -- ---- end listing 8.2 ----
    """
    lines = path.read_text().splitlines()
    opened = _index(lines, lambda line: line.startswith("-- ---- Listing %s:" % number))
    closed = _index(lines, lambda line: line.startswith("-- ---- end listing %s " % number))
    if opened is None or closed is None:
        raise LookupError("%s does not fence listing %s" % (path.name, number))
    return _trim(lines[opened + 1:closed])


def _index(lines, matches, start=0):
    for n in range(start, len(lines)):
        if matches(lines[n]):
            return n
    return None


def _trim(lines):
    """Drop trailing spaces and the blank lines at either edge. Nothing else."""
    body = [line.rstrip() for line in lines]
    while body and not body[0]:
        body.pop(0)
    while body and not body[-1]:
        body.pop()
    return "\n".join(body)
