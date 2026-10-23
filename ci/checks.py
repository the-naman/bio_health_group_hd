"""CI checks for a pull request into test or prod (same file in both repos; needs pyyaml and ruff). Run from the repo root:
    python ci/checks.py
Fails (exit 1) when any check fails. Prints file names and line numbers only, never the matched text."""

import ast
import glob
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

sys.path.insert(0, "src")
from lh import jobs  # noqa: E402

EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}")
TOKEN = re.compile(r"dapi[0-9a-f]{32}|AKIA[0-9A-Z]{16}|ghp_[A-Za-z0-9]{36}|github_pat_\w{20,}|BEGIN [A-Z ]*PRIVATE KEY")
CATALOG = re.compile(r"\b(bhg|fqf)_(dev|test|prod)\.")
problems, passed = [], []


def check(name, bad):
    """bad = list of problems found (empty = pass)."""
    if bad:
        problems.append(name)
        print(f"FAIL {name}")
        for b in bad[:20]:
            print(f"     {b}")
    else:
        passed.append(name)
        print(f"PASS {name}")


files = subprocess.check_output(["git", "ls-files"], text=True).split()
code = yaml.safe_load(Path("config/company.yml").read_text())["code"]

# 1. Python files and notebook code cells compile (magics like %pip and !cmd are skipped)
bad = []
for f in [f for f in files if f.endswith(".py")]:
    try:
        ast.parse(Path(f).read_text(), f)
    except SyntaxError as e:
        bad.append(f"{f}:{e.lineno} {e.msg}")
for f in [f for f in files if f.endswith(".ipynb")]:
    for i, c in enumerate(json.loads(Path(f).read_text())["cells"]):
        if c.get("cell_type") != "code":
            continue
        src = "".join(c["source"])
        src = "\n".join("" if line.lstrip().startswith(("%", "!")) else line for line in src.splitlines())
        try:
            ast.parse(src)
        except SyntaxError as e:
            bad.append(f"{f} cell {i + 1}:{e.lineno} {e.msg}")
check("python and notebooks compile", bad)

# 2. Notebooks are committed without outputs (outputs can hold data)
bad = [
    f
    for f in files
    if f.endswith(".ipynb")
    and any(c.get("outputs") for c in json.loads(Path(f).read_text())["cells"] if c.get("cell_type") == "code")
]
check("notebooks have no saved outputs", bad)

# 3. YAML is valid (config and workflows)
bad = []
for f in [f for f in files if f.endswith((".yml", ".yaml"))]:
    try:
        yaml.safe_load(Path(f).read_text())
    except yaml.YAMLError as e:
        bad.append(f"{f}: {str(e).splitlines()[0]}")
check("yaml files parse", bad)

# 4. The daily job graph is valid: dependencies, loops, run_if, notebooks exist, notify settings
try:
    jcfg = jobs.load_jobs(".")
    jobs.check(jobs.tasks_for(jcfg, code), ".", jcfg)
    for env in ("test", "prod"):
        jobs.build(jcfg, code, env, "/Users/ci", "00000000-0000-0000-0000-000000000000")
    jobs.build(jcfg, code, "dev", "/Users/ci", role=sorted(jcfg["dev_roles"].values())[0])
    bad = []
except Exception as e:  # noqa: BLE001 - any error in building a job is reported as a failed check
    bad = [f"{type(e).__name__}: {e}"]
check("config/jobs.yml builds a valid job for dev, test and prod", bad)

# 5. Dashboards are valid JSON and name only dev catalogs (publishing swaps them per environment)
bad = []
for f in sorted(glob.glob("bi/*.lvdash.json")):
    try:
        text = Path(f).read_text()
        json.loads(text)
    except ValueError as e:
        bad.append(f"{f}: not valid JSON ({e})")
        continue
    other = sorted({f"{m.group(1)}_{m.group(2)}" for m in CATALOG.finditer(text) if m.group(2) != "dev"})
    if other:
        bad.append(f"{f}: names {other}; dashboards must use the _dev catalogs only")
check("dashboards valid and dev-catalog only", bad)

# 6. No email address and no token in any committed text file (positions only, never the text)
bad_mail, bad_token = [], []
for f in files:
    try:
        text = Path(f).read_text(encoding="utf-8")
    except (UnicodeDecodeError, IsADirectoryError, FileNotFoundError):
        continue
    for n, line in enumerate(text.splitlines(), 1):
        if EMAIL.search(line):
            bad_mail.append(f"{f}:{n}")
        if TOKEN.search(line):
            bad_token.append(f"{f}:{n}")
check("no email address in any file", bad_mail)
check("no token or private key in any file", bad_token)

# 7. Python style: ruff lint and format, rules in pyproject.toml (notebooks excluded: thin runners, compiled in check 1)
RUFF = [shutil.which("ruff")] if shutil.which("ruff") else [sys.executable, "-m", "ruff"]
lint = subprocess.run([*RUFF, "check", "--output-format", "concise", "."], capture_output=True, text=True)
fmt = subprocess.run([*RUFF, "format", "--check", "."], capture_output=True, text=True)
if "No module named ruff" in lint.stderr + fmt.stderr:
    bad = ["ruff is not installed (pip install ruff==0.15.11)"]
else:
    bad = [x for x in lint.stdout.splitlines() if x and not x.startswith(("Found", "All checks", "["))]
    bad += [x for x in fmt.stdout.splitlines() if x.startswith("Would reformat")]
check("python lint and format (ruff)", bad)

print(f"\n{len(passed)}/{len(passed) + len(problems)} PASS")
sys.exit(1 if problems else 0)
