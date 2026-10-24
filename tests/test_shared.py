"""The shared-code drift guard (ci/shared.py) finds every kind of drift, offline with two small git folders."""

import subprocess
import sys

from tests.conftest import ROOT

sys.path.insert(0, str(ROOT / "ci"))
import shared  # noqa: E402

CFG = {
    "roots": ["src/lh/", "ci/"],
    "files": ["config/ci.yml"],
    "per_company": ["ci/own.yml"],
    "company_only": {"bhg": ["src/lh/group.py"], "fqf": ["src/lh/stream.py"]},
}


def repo(path, files):
    path.mkdir()
    for name, text in files.items():
        (path / name).parent.mkdir(parents=True, exist_ok=True)
        (path / name).write_text(text)
    subprocess.run(["git", "init", "-q", str(path)], check=True)
    subprocess.run(["git", "-C", str(path), "add", "-A"], check=True)
    return path


BASE = {"src/lh/common.py": "x = 1\n", "ci/checks.py": "y = 2\n", "config/ci.yml": "a: 1\n", "README.md": "bhg\n"}


def test_identical_repos_pass(tmp_path):
    a = repo(tmp_path / "a", {**BASE, "src/lh/group.py": "g\n", "ci/own.yml": "bhg\n"})
    b = repo(tmp_path / "b", {**BASE, "src/lh/stream.py": "s\n", "ci/own.yml": "fqf\n", "README.md": "fqf\n"})
    assert shared.compare(a, b, "bhg", "fqf", CFG) == []


def test_a_changed_shared_file_is_caught(tmp_path):
    a = repo(tmp_path / "a", BASE)
    b = repo(tmp_path / "b", {**BASE, "src/lh/common.py": "x = 2\n"})
    assert shared.compare(a, b, "bhg", "fqf", CFG) == [("differs", "src/lh/common.py")]


def test_a_new_file_forgotten_in_the_other_repo_is_caught(tmp_path):
    a = repo(tmp_path / "a", {**BASE, "ci/new.py": "n\n"})
    b = repo(tmp_path / "b", BASE)
    assert shared.compare(a, b, "bhg", "fqf", CFG) == [("only in bhg, not declared", "ci/new.py")]


def test_company_only_file_in_the_wrong_repo_is_caught(tmp_path):
    a = repo(tmp_path / "a", {**BASE, "src/lh/stream.py": "s\n"})
    b = repo(tmp_path / "b", BASE)
    assert shared.compare(a, b, "bhg", "fqf", CFG) == [("only in bhg, not declared", "src/lh/stream.py")]


def test_the_two_real_repos_declare_each_others_names():
    import yaml

    cfg = yaml.safe_load((ROOT / "config" / "ci.yml").read_text())["shared"]
    assert set(cfg["repos"]) == {"bhg", "fqf"} and set(cfg["company_only"]) == {"bhg", "fqf"}
