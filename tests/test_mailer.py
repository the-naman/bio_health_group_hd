"""ci/mailer.py (28d): every platform mail is HTML with a plain-text first part, an inline animated icon and no
external content; the plain text is exactly what the senders wrote before."""

import re
import runpy

import pytest

from tests.conftest import ROOT

M = runpy.run_path(str(ROOT / "ci" / "mailer.py"), run_name="mailer")
build, render, blocks, TONES, ASSETS = M["build"], M["render"], M["blocks"], M["TONES"], M["ASSETS"]
RUN = "https://github.com/o/r/actions/runs/9"
PR = "https://github.com/o/r/pull/41"
KW = {"kicker": "Title", "company": "Flexi Quant Finance", "env": "prod", "footer": "Please do not reply."}


def test_structure_plain_first_html_second_icon_inline():
    msg = build("Subject line", "Body.\n\nRun: " + RUN, tone="fail", **KW)
    assert msg.get_content_type() == "multipart/alternative"
    assert [p.get_content_type() for p in msg.walk()] == [
        "multipart/alternative",
        "text/plain",
        "multipart/related",
        "text/html",
        "image/gif",
    ]
    assert msg.get_body(("plain",)).get_content() == "Body.\n\nRun: " + RUN + "\n\n-- Please do not reply.\n"
    gif = [p for p in msg.walk() if p.get_content_type() == "image/gif"][0]
    html = msg.get_body(("html",)).get_content()
    assert f'src="cid:{gif["Content-ID"].strip("<>")}"' in html


@pytest.mark.parametrize("tone", sorted(TONES))
def test_every_tone_has_an_animated_icon(tone):
    data = (ASSETS / f"{tone}.gif").read_bytes()
    assert data[:6] == b"GIF89a" and data.count(b"\x21\xf9\x04") > 1  # more than one frame
    assert len(data) < 40_000


def test_no_external_content_no_script():
    html = render("S", "Line with " + RUN, tone="info", icon_cid="x", **KW)
    assert "<script" not in html.lower()
    assert not re.search(r"""src=["']https?:""", html)  # no remote images, no tracking pixel
    assert "@import" not in html and "url(" not in html


def test_text_is_escaped():
    html = render("<b>S</b>", "a <script>x</script> & b", tone="info", icon_cid=None, **KW)
    assert "<script>x" not in html and "&lt;script&gt;" in html and "&lt;b&gt;S" in html


def test_layout_rules():
    body = (
        "First line.\n\nWhat to do:\n1. One.\n2. Two.\n\nChanges:\n  - a\n  - b\n\n"
        "Figures:\n  loans paid out (INR)      1,000.00   2 loans\n  late fees                 10.00\n\n"
        "Pull request: x\n" + PR + "\nThe deploy failed. Details: " + RUN
    )
    kinds = [k for k, _ in blocks(body)]
    assert kinds == ["p", "h", "ol", "h", "ul", "h", "table", "p", "button", "p", "button"]
    html = render("S", body, tone="ok", icon_cid=None, **KW)
    assert "Open the pull request" in html and "Open details" in html


def test_urgent_is_yellow_with_black_on_red():
    html = render("S", "b", tone="urgent", icon_cid=None, **KW)
    assert "#f2c230" in html and "#ef4444" in html and "#111111" in html


def test_unknown_tone_is_refused():
    with pytest.raises(ValueError):
        build("S", "b", tone="pink", **KW)


NOTIFY = runpy.run_path(str(ROOT / "ci" / "notify.py"), run_name="notify")


@pytest.mark.parametrize(
    ("flow", "event", "subject", "ok", "expected"),
    [
        ("promotion", "request", "Approval needed: a -> b", True, ("info", "Approval needed")),
        ("hotfix", "request", "Approval needed: h -> b", True, ("urgent", "URGENT · Hotfix approval")),
        ("promotion", "result", "Approved: a -> b", True, ("ok", "Approved")),
        ("promotion", "result", "Rejected: a -> b", True, ("fail", "Rejected")),
        ("promotion", "result", "Checks failed: a -> b", True, ("fail", "Checks failed")),
        ("promotion", "deployed", "Deployed x to b", True, ("ok", "Deployed")),
        ("hotfix", "deployed", "Deployed x to b", True, ("warn", "Hotfix deployed")),
        ("promotion", "deployed", "Deploy FAILED for b", False, ("fail", "Deploy failed")),
        ("hotfix", "backmerge", "Back-merge needed: p -> d", True, ("warn", "Back-merge needed")),
    ],
)
def test_notify_look(flow, event, subject, ok, expected):
    assert NOTIFY["look"](flow, event, subject, ok) == expected


@pytest.mark.parametrize("script", ["notify", "alerts", "notices", "watchdog"])
def test_every_sender_uses_the_mailer(script):
    text = (ROOT / "ci" / f"{script}.py").read_text()
    assert "from mailer import build" in text and "EmailMessage()" not in text and "set_content(" not in text


@pytest.mark.parametrize(
    "body",
    [
        "Runs:  3 steps\n  bronze.payments                1 not ok   last: rejected",  # one table line (digest)
        "  a   b",
        "x:\n",
        ":",
        "  -",
        "1.",
        "Run: https://x",
        "  a  b\nplain\n  c  d\n  - item\n1. step\nlast:",
        "\n\n\n",
        "",
    ],
)
def test_layout_always_finishes_and_keeps_all_text(body):
    out = blocks(body)
    text = " ".join(str(d) for _, d in out)
    for word in re.findall(r"[a-z]{3,}", body):
        assert word in text
