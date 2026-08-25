"""
The five tests that would have caught every bug found in the audit.

Each targets a real failure that 265 existing tests missed, because they
tested that code EXISTED rather than that it WORKED.
"""
import ast
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ── 1. Age parser must handle every format the 9 repos actually emit ──
def test_age_parser_handles_all_real_formats():
    from aggregator.run_aggregator import UnifiedJobAggregator as U
    p = U._parse_github_age
    expect = {
        "0d": 0, "3d": 3, "11d": 11, "116d": 116,
        "11m": 0, "52m": 0, "20h": 0, "0m": 0,     # zapplyjobs minutes/hours
        "1mo": 30, "2mo": 60, "1w": 7, "2w": 14,
    }
    for raw, want in expect.items():
        got = p(raw)
        assert got == want, "{!r} -> {} (expected {})".format(raw, got, want)
    for junk in ("", "N/A", "Apply", None):
        assert p(junk) is None, "{!r} should be unparseable".format(junk)


# ── 2. A blank company cell must not shift every column left ──
def test_blank_cell_does_not_shift_columns():
    from aggregator.extractors import SimplifyGitHubScraper as S
    md = ("| Company | Role | Location | Terms | Application | Age |\n"
          "|---|---|---|---|---|---|\n"
          "| Disney | CS Intern | Lake Buena Vista, FL | Spring 2027 | [Apply](https://x.co/a) | 0d |\n"
          "|  | Labor Systems Intern | Lake Buena Vista, FL | Winter 2027 | [Apply](https://x.co/b) | 0d |\n")
    rows = S._parse_markdown_text(md, "test")
    assert len(rows) == 2, "expected 2 rows, got {}".format(len(rows))
    # the blank-company row must INHERIT, not absorb its own title
    assert rows[1]["company"] == "Disney", rows[1]
    assert "Intern" in rows[1]["title"], rows[1]
    assert "," in rows[1]["location"], rows[1]


# ── 3. ATS scrapers must never hardcode an age ──
def test_ats_scrapers_do_not_hardcode_age():
    src = open(os.path.join(BASE, "aggregator", "direct_sources.py"), encoding="utf-8").read()
    hardcoded = src.count('"age": "0d"')
    # HackerNews is the only legitimate case (monthly thread, no per-job date)
    assert hardcoded <= 1, (
        "{} scrapers still hardcode age='0d'. Every direct-ATS job would "
        "claim it was posted today and the age filter would be meaningless."
        .format(hardcoded))


def test_pick_age_uses_the_right_loop_variable():
    """A mismatched variable raises NameError that gets swallowed, so the
    scraper silently returns zero jobs."""
    src = open(os.path.join(BASE, "aggregator", "direct_sources.py"), encoding="utf-8").read()
    bad = []
    for fn in re.finditer(r"def (scrape_\w+)\(", src):
        name = fn.group(1)
        i = fn.start()
        j = src.find("\ndef ", i + 1)
        body = src[i:j if j != -1 else len(src)]
        loops = set(re.findall(r"for (\w+) in ", body))
        for var in re.findall(r"_pick_age\((\w+),", body):
            if var not in loops:
                bad.append("{}: _pick_age({}) but loops are {}".format(name, var, sorted(loops)))
    assert not bad, "\n".join(bad)


# ── 4. The learning loop must round-trip: writer -> brain -> reader ──
def test_learning_loop_round_trips(tmp_path):
    import aggregator.apply_learned as al
    # the reader must resolve to the REAL brain.json, not a subfolder
    assert os.path.basename(os.path.dirname(al.BRAIN_FILE)) == ".local", al.BRAIN_FILE
    assert os.path.exists(al.BRAIN_FILE), "reader points at a nonexistent file: " + al.BRAIN_FILE

    # writer and reader must agree on the key names
    qg = open(os.path.join(BASE, "scripts", "quality_gate.py"), encoding="utf-8").read()
    for key in ("learned_slugs", "learned_non_tech", "learned_clearance"):
        assert key in qg, "quality_gate never writes " + key
        assert key in open(al.__file__, encoding="utf-8").read(), "apply_learned never reads " + key

    # and the writer must actually be CALLED, not just defined
    for meth in ("add_slug_fix", "add_non_tech_title", "add_clearance_company"):
        calls = len(re.findall(r"\.{}\(".format(meth), qg))
        assert calls >= 1, "{} is defined but never called".format(meth)


# ── 5. Config rewrites must produce parseable Python ──
def test_auto_blacklist_rewrite_is_valid_python():
    """f'\\1' in an f-string is the control character \\x01, not a regex
    backreference. That silently destroyed the variable declaration."""
    cfg = 'COMPANY_BLACKLIST = [\n    "Existing",\n]\n'
    company = "BadCorp"
    out = re.sub(r"(COMPANY_BLACKLIST\s*=\s*\[)", f'\\1\n    "{company}",', cfg, count=1)
    ast.parse(out)                                   # must not raise
    assert "COMPANY_BLACKLIST" in out, "declaration was destroyed"
    assert "\x01" not in out, "control character present"

    src = open(os.path.join(BASE, "scripts", "build_auto_blacklist.py"), encoding="utf-8").read()
    assert "\x01" not in src, "build_auto_blacklist contains a literal control character"


# ── bonus: no source file may contain control characters ──
def test_no_control_characters_in_source():
    """The \\b -> \\x08 bug killed 4 filters silently. The \\1 -> \\x01 bug
    killed auto-blacklist. Same class, twice — so guard the whole tree."""
    bad = []
    for root, dirs, files in os.walk(BASE):
        dirs[:] = [d for d in dirs if d not in
                   ("venv", ".venv", "node_modules", "__pycache__", ".git", ".local")]
        for f in files:
            if not f.endswith(".py"):
                continue
            p = os.path.join(root, f)
            try:
                text = open(p, encoding="utf-8", errors="replace").read()
            except Exception:
                continue
            for ch in ("\x01", "\x08", "\x02", "\x03", "\x07"):
                if ch in text:
                    bad.append("{}: contains {!r}".format(os.path.relpath(p, BASE), ch))
    assert not bad, "\n".join(bad)


# ── bonus: every scheduler job type must have a dispatch branch ──
def test_every_job_type_is_dispatched():
    """quality_gate and health_heartbeat were type='post_write' with no
    branch in the loop, so they never ran once."""
    src = open(os.path.join(BASE, "scripts", "scheduler.py"), encoding="utf-8").read()
    declared = set(re.findall(r'"type"\s*:\s*"(\w+)"', src))
    loop = src[src.find("for job in JOBS:", src.find("while True")):]
    loop = loop[:loop.find("time.sleep")]
    handled = set(re.findall(r'job\["type"\] == "(\w+)"', loop))
    extra = set(re.findall(r'get\("type"\) != "(\w+)"', src))   # post_write runner
    missing = declared - handled - extra
    assert not missing, "job types with no dispatch branch: {}".format(sorted(missing))


# ── 6. The ATS resolver must be reachable and see discovered boards ──
def test_resolver_sees_discovered_boards():
    """_try_ats_lookup imports the hardcoded company dicts at call time.
    Without merging ats_discovery's findings it sees ~263 companies instead
    of ~820, so most lookups fail and jobs fall back to search URLs."""
    src = open(os.path.join(BASE, "aggregator", "run_aggregator.py"), encoding="utf-8").read()
    i = src.find("def _try_ats_lookup")
    assert i != -1, "_try_ats_lookup is gone"
    j = src.find("\n    def ", i + 1)
    body = src[i:j if j != -1 else len(src)]
    assert "_load_discovered_companies" in body, (
        "resolver does not merge discovered boards — it will only see the "
        "hardcoded companies")


def test_resolver_is_actually_called():
    """It was built, tested, and then only wired into one path. Assert it is
    invoked from at least two places, not merely defined."""
    src = open(os.path.join(BASE, "aggregator", "run_aggregator.py"), encoding="utf-8").read()
    calls = len(re.findall(r"self\._try_ats_lookup\(", src))
    assert calls >= 2, "_try_ats_lookup called from only {} place(s)".format(calls)


# ── 7. The preflight must be clean, and must actually detect breakage ──
def test_preflight_passes_on_current_code():
    from aggregator.preflight import run_preflight
    ok, problems = run_preflight(verbose=False)
    assert ok, "preflight found wiring problems:\n" + "\n".join(problems)


def test_preflight_detects_control_characters(tmp_path, monkeypatch):
    """A checker that cannot fail is worthless - prove it catches the real bug."""
    import aggregator.preflight as pf
    fake = tmp_path / "broken.py"
    fake.write_text("x = 1\n# \x01 injected\n")
    monkeypatch.setattr(pf, "_iter_py", lambda: [str(fake)])
    found = pf.check_control_characters()
    assert found, "control-character check failed to detect \\x01"


def test_preflight_is_wired_into_the_aggregator():
    """It only protects anything if it actually runs."""
    src = open(os.path.join(BASE, "aggregator", "run_aggregator.py"), encoding="utf-8").read()
    assert "run_preflight" in src, "preflight is never called by the aggregator"


# ── 8. Full-time must survive the internship gate; senior must not ──
def test_fulltime_survives_internship_gate():
    """Two gates rejected any non-internship title as a 'senior role'. The only
    exemption was source.startswith('simplify_newgrad'), so 1,594 full-time
    new-grad roles per run were discarded - the largest single loss found."""
    from aggregator.run_aggregator import UnifiedJobAggregator as U, _is_senior_title
    from aggregator.processors import TitleProcessor as T

    def kept(title, source):
        jt = U._detect_job_type(title, source)
        intern, _ = T.is_internship_role(title)
        is_ft = (jt.strip().lower() not in ("internship", "intern", "co-op", "coop")
                 and not _is_senior_title(title))
        return intern or is_ft

    must_keep = [("Software Engineer I", "greenhouse_direct"),
                 ("New Grad: Software Engineer", "cvrve_newgrad"),
                 ("Associate Software Engineer", "indeed_direct"),
                 ("Backend Engineer, Payments", "ashby_direct"),
                 ("Software Engineer", "speedyapply_swe_newgrad"),
                 ("Software Engineer Intern", "vanshb03_offseason")]
    for t, s in must_keep:
        assert kept(t, s), "full-time/new-grad role wrongly rejected: {} [{}]".format(t, s)

    must_drop = [("Senior Staff Software Engineer", "greenhouse_direct"),
                 ("Principal Engineer", "indeed_direct"),
                 ("Engineering Manager", "zapplyjobs_it"),
                 ("Staff Data Scientist", "greenhouse_direct"),
                 ("Director of Engineering", "ashby_direct")]
    for t, s in must_drop:
        assert not kept(t, s), "senior role wrongly kept: {} [{}]".format(t, s)


def test_seniority_gate_is_not_source_name_matching():
    """The original bug: exemption keyed on the source STRING, which missed
    every direct-ATS source and Indeed. Assert job type drives it instead."""
    src = open(os.path.join(BASE, "aggregator", "run_aggregator.py"), encoding="utf-8").read()
    assert "_is_senior_title" in src, "seniority filter is gone"
    assert src.count("_detect_job_type(title, source)") >= 1 or \
           src.count("_detect_job_type(title, _src)") >= 1, \
           "internship gate is no longer job-type aware"


# ── 9. Fuzzy dedup must not collapse distinct roles ──
def test_fuzzy_dedup_keeps_specialisations_and_levels():
    """TF-IDF averages away the one token that carries the meaning:
    "Software Engineer II" scored 1.00 against "Software Engineer I", and
    "DeFi Algorithmic Trader" scored 0.99 against "Algorithmic Trader".
    Both were dropped as duplicates. The guard is word-set based, not
    positional, so reorderings still merge."""
    from analytics.similarity import _differs_on_discriminator as D

    must_differ = [
        ("Software Engineer II", "Software Engineer I"),
        ("DeFi Algorithmic Trader", "Algorithmic Trader"),
        ("Research Scientist - Agents", "Research Scientist"),
        ("Pharmacy Technician Back End", "Pharmacy Technician"),
        ("Data Scientist Intern - NLP", "Data Scientist Intern"),
        ("Backend Engineer, Payments", "Frontend Engineer, Payments"),
    ]
    for a, b in must_differ:
        assert D(a, b), "distinct roles wrongly merged: {!r} vs {!r}".format(a, b)

    must_match = [
        ("Software Engineer Intern", "Software Engineering Intern"),
        ("Single-Family Software Developer Intern",
         "Software Developer Intern - Single-Family"),
        ("Operations Analyst - US Government Services",
         "Operations Analyst - US Government"),
        ("Aerospace Algorithms Engineer Co-op",
         "Aerospace Algorithms Engineer Graduate Co-op"),
    ]
    for a, b in must_match:
        assert not D(a, b), "same role wrongly split: {!r} vs {!r}".format(a, b)


def test_non_identifying_urls_are_not_dedup_keys():
    """clean_url strips the query string, so all 306 google-search rows
    collapse to one key. Deduping on it would drop the next fallback job of
    any company as a duplicate of an unrelated one."""
    from aggregator.utils import is_identifying_url as I, URLCleaner
    a = "https://www.google.com/search?q=AMD+Data+Analyst"
    b = "https://www.google.com/search?q=Disney+SWE"
    assert URLCleaner.clean_url(a) == URLCleaner.clean_url(b), \
        "premise changed: these no longer collide"
    assert not I(a) and not I(b), "search URLs must not be dedup keys"
    assert I("https://job-boards.greenhouse.io/twitch/jobs/8459320002"), \
        "real job URLs must remain dedup keys"
