"""Databricks Asset Bundle file of this repo (same file in both repos; docs/adr/0011). Run from the repo root:
    python ci/bundle.py --write                  # regenerate databricks.yml from config/jobs.yml and config/stream.yml
    python ci/bundle.py --check                  # fail if databricks.yml differs from what the config generates
    python ci/bundle.py --check --schema s.json  # also validate it against `databricks bundle schema` (needs jsonschema, regex)
Prints file names and problems only."""

import argparse
import json
import sys
from pathlib import Path

import yaml

sys.path.insert(0, "src")
from lh import bundle  # noqa: E402

FILE = Path("databricks.yml")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--write", action="store_true")
    p.add_argument("--check", action="store_true")
    p.add_argument("--schema")
    a = p.parse_args()
    code = yaml.safe_load(Path("config/company.yml").read_text())["code"]
    want = bundle.render(".", code)
    if a.write:
        FILE.write_text(want)
        print(f"wrote {FILE}")
    if a.check:
        if not FILE.exists() or FILE.read_text() != want:
            print(f"FAIL {FILE} is not what config generates: run python ci/bundle.py --write and commit it")
            sys.exit(1)
        print(f"PASS {FILE} = generated from config")
    if a.schema:
        schema = json.loads(Path(a.schema).read_text())
        errors = list(bundle.validator(schema).iter_errors(yaml.safe_load(FILE.read_text())))
        for e in errors[:20]:
            while e.context:  # a oneOf/anyOf error: report the branch that got furthest into the file
                e = max(e.context, key=lambda c: len(c.absolute_path))
            print(f"FAIL {'/'.join(map(str, e.absolute_path))}: {e.message[:120]}")
        if errors:
            sys.exit(1)
        print(f"PASS {FILE} valid against the bundle schema")


if __name__ == "__main__":
    main()
