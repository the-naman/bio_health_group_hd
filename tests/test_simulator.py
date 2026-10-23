"""The simulator is deterministic: the same company, environment and date give byte-identical files."""

import hashlib
import subprocess
import sys

from tests.conftest import CODE, ROOT


def digest(folder):
    h = hashlib.sha256()
    files = sorted(p for p in folder.rglob("*") if p.is_file())
    for p in files:
        h.update(str(p.relative_to(folder)).encode() + p.read_bytes())
    return len(files), h.hexdigest()


def test_same_input_same_files(tmp_path):
    runs = []
    for i in (1, 2):
        out = tmp_path / f"run{i}"
        cmd = [
            sys.executable,
            "-m",
            "simulator.run",
            "--company",
            CODE,
            "--env",
            "dev",
            "--out",
            str(out),
            "--upto",
            "2026-10-06",
        ]
        subprocess.run(cmd, cwd=ROOT, check=True, capture_output=True, timeout=600)
        runs.append(digest(out))
    assert runs[0][0] > 100, runs
    assert runs[0] == runs[1]
