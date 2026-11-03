"""HTML mails for every platform sender (28d, same file in both repos).

One look for all mails (notify, alerts, notices, watchdog): a coloured header with a small animated status icon, the
company and an environment badge, then the existing plain-text body laid out as HTML. Each message is
multipart/alternative: the plain text stays as the first part (old clients, previews, tests), the HTML second, with the
icon attached inline by Content-ID. No external images, no tracking pixels, no web fonts, no scripts; only tables and
inline styles, the subset Gmail, Outlook and phone apps all render. Python standard library only.

How the plain body is read (the senders keep writing plain text):
    blank line            -> new block
    "Title:" + "  - x"    -> small heading + bullet list
    "1. x"                -> numbered steps
    "  a   b   c"         -> table (indented lines whose columns are split by 2+ spaces)
    "Run: https://..."    -> button (one-word label, then a link)
    other https:// links  -> clickable links

    from mailer import build
    msg = build(subject, body, tone="fail", kicker="Deploy failed", company="Flexi Quant Finance", env="prod",
                footer="Lakehouse operations alert. Please do not reply.")
    msg["From"], msg["To"] = sender, recipients
"""

import html
import re
from email.message import EmailMessage
from email.utils import make_msgid
from pathlib import Path

ASSETS = Path(__file__).resolve().parent / "mail_assets"
TONES = {  # header colour, soft panel colour, text colour on the panel
    "ok": ("#1f8a4c", "#e8f5ee", "#14532d"),
    "warn": ("#b7791f", "#fdf3e1", "#713f12"),
    "fail": ("#c0392b", "#fdecea", "#7f1d1d"),
    "info": ("#2f6fde", "#e8f0fd", "#1e3a8a"),
    "urgent": (
        "#f2c230",
        "#ef4444",
        "#111111",
    ),  # urgent hotfix: yellow, black text on red labels
}
INK = {"urgent": "#111111"}  # text on the header and buttons; white for the rest
ACCENT = {"urgent": "#b91c1c"}  # headings, bullets, panel edge on white
LABEL = {"urgent": "#ef4444"}  # urgent: red label behind the title and the button
ENV_BADGE = {"prod": "#111827", "test": "#6b21a8", "dev": "#475569"}
FONT = "-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif"
MONO = "SFMono-Regular,Consolas,'Liberation Mono',Menlo,monospace"
URL = re.compile(r"https://[^\s<>\"')]+")
BUTTON = re.compile(r"^(?:([A-Z][A-Za-z ]{0,24}):\s+)?(https://\S+)$")
STEP = re.compile(r"^(\d{1,2})\.\s+(.*)$")
ITEM = re.compile(r"^\s+-\s+(.*)$")
COLS = re.compile(r"\s{2,}")
TRAIL = re.compile(r"[ \t]+(Details|Run):[ \t]+(https://\S+)[ \t]*$", re.M)
NUM = re.compile(r"^[\d,.\-]+")


def esc(text):
    """Escape, then turn https links into anchors (the link text is the escaped URL itself)."""
    out, last = [], 0
    for m in URL.finditer(text):
        out.append(html.escape(text[last : m.start()]))
        u = html.escape(m.group(0))
        out.append(f'<a href="{u}" style="color:#2f6fde;word-break:break-all">{u}</a>')
        last = m.end()
    out.append(html.escape(text[last:]))
    return "".join(out)


def is_row(line):
    return line.startswith("  ") and not ITEM.match(line) and len(COLS.split(line.strip())) >= 2


def blocks(body):
    """Plain body -> list of (kind, data). kinds: p, h, ul, ol, table, button."""
    out = []
    body = TRAIL.sub(r"\n\n\1: \2", body)  # "... failed. Details: https://..." -> its own button line
    for chunk in re.split(r"\n\s*\n", body.strip("\n")):
        lines = chunk.split("\n")
        i = 0
        while i < len(lines):
            line = lines[i].rstrip()
            if not line.strip():
                i += 1
                continue
            if ITEM.match(line):
                items = []
                while i < len(lines) and ITEM.match(lines[i]):
                    items.append(ITEM.match(lines[i]).group(1))
                    i += 1
                out.append(("ul", items))
            elif STEP.match(line):
                items = []
                while i < len(lines) and STEP.match(lines[i]):
                    items.append(STEP.match(lines[i]).group(2))
                    i += 1
                out.append(("ol", items))
            elif is_row(line):  # one or more indented column lines
                rows = []
                while i < len(lines) and is_row(lines[i]):
                    rows.append(COLS.split(lines[i].strip()))
                    i += 1
                out.append(("table", rows))
            elif BUTTON.match(line.strip()):
                label, url = BUTTON.match(line.strip()).groups()
                out.append(("button", (label, url)))
                i += 1
            elif (
                line.endswith(":")
                and i + 1 < len(lines)
                and (ITEM.match(lines[i + 1]) or STEP.match(lines[i + 1]) or is_row(lines[i + 1]))
            ):
                out.append(("h", line[:-1]))
                i += 1
            else:  # paragraph: consecutive plain lines, kept as line breaks
                para = []
                while (
                    i < len(lines)
                    and lines[i].strip()
                    and not (
                        ITEM.match(lines[i])
                        or STEP.match(lines[i])
                        or BUTTON.match(lines[i].strip())
                        or is_row(lines[i])
                    )
                ):
                    if (
                        lines[i].rstrip().endswith(":")
                        and i + 1 < len(lines)
                        and (ITEM.match(lines[i + 1]) or STEP.match(lines[i + 1]) or is_row(lines[i + 1]))
                    ):
                        break
                    para.append(lines[i].strip())
                    i += 1
                if not para:  # safety net: always move on, whatever the line looks like
                    para, i = [line.strip()], i + 1
                out.append(("p", para))
    return out


def part(kind, data, tone):
    head, soft, deep = TONES[tone]
    ink = INK.get(tone, "#ffffff")
    accent = ACCENT.get(tone, head)
    p = f"margin:0 0 14px;font:15px/1.55 {FONT};color:#1f2937"
    if kind == "p":
        return f'<p style="{p}">' + "<br>".join(esc(x) for x in data) + "</p>"
    if kind == "h":
        return (
            f'<p style="margin:18px 0 8px;font:600 12px/1.4 {FONT};letter-spacing:.08em;text-transform:uppercase;'
            f'color:{accent}">{esc(data)}</p>'
        )
    if kind == "ul":
        rows = "".join(
            f'<tr><td valign="top" style="padding:3px 10px 3px 0;color:{accent};font:15px/1.55 {FONT}">&#9679;</td>'
            f'<td style="padding:3px 0;font:15px/1.55 {FONT};color:#1f2937">{esc(x)}</td></tr>'
            for x in data
        )
        return f'<table role="presentation" cellpadding="0" cellspacing="0" style="margin:0 0 14px">{rows}</table>'
    if kind == "ol":
        rows = "".join(
            f'<tr><td valign="top" style="padding:4px 12px 4px 0"><div style="width:24px;height:24px;border-radius:12px;'
            f'background:{head};color:{ink};font:700 12px/24px {FONT};text-align:center">{n}</div></td>'
            f'<td style="padding:4px 0 8px;font:15px/1.55 {FONT};color:#1f2937">{esc(x)}</td></tr>'
            for n, x in enumerate(data, 1)
        )
        return (
            f'<table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="margin:0 0 14px;'
            f'background:{soft};border-radius:10px"><tr><td style="padding:12px 14px"><table role="presentation" '
            f'cellpadding="0" cellspacing="0">{rows}</table></td></tr></table>'
        )
    if kind == "table":
        width = max(len(r) for r in data)
        body = ""
        for n, r in enumerate(data):
            cells = r + [""] * (width - len(r))
            bg = "#f9fafb" if n % 2 else "#ffffff"
            body += (
                "<tr>"
                + "".join(
                    f'<td style="padding:7px 10px;border-bottom:1px solid #eef0f3;background:{bg};font:13px/1.45 {MONO if k else FONT};'
                    f'color:#1f2937;{"text-align:right;white-space:nowrap;" if k and NUM.match(c) else ""}">'
                    f"{esc(c)}</td>"
                    for k, c in enumerate(cells)
                )
                + "</tr>"
            )
        return (
            '<div style="overflow-x:auto;margin:0 0 14px"><table role="presentation" cellpadding="0" cellspacing="0" '
            f'width="100%" style="border:1px solid #e5e7eb;border-radius:8px;border-collapse:separate">{body}</table></div>'
        )
    if kind == "button":
        label, url = data
        if not label:
            label = "Pull request" if "/pull/" in url else "Run" if "/actions/runs/" in url else "Link"
        text = {
            "Run": "Open the run",
            "Details": "Open details",
            "Link": "Open",
            "Pull request": "Open the pull request",
        }.get(label, f"Open {label.lower()}")
        u = html.escape(url)
        return (
            '<table role="presentation" cellpadding="0" cellspacing="0" style="margin:6px 0 16px"><tr>'
            f'<td style="border-radius:8px;background:{LABEL.get(tone, head)}"><a href="{u}" style="display:inline-block;padding:11px 20px;'
            f'font:600 14px/1 {FONT};color:{ink};text-decoration:none;border-radius:8px">{html.escape(text)} &rarr;</a>'
            f'</td></tr></table><p style="margin:-8px 0 14px;font:12px/1.4 {FONT};color:#6b7280;word-break:break-all">'
            f"{u}</p>"
        )
    raise ValueError(kind)


def render(subject, body, *, tone, kicker, company, env, footer, icon_cid="icon"):
    """The HTML document. icon_cid: Content-ID of the inline GIF (without <>), or None to leave the icon out."""
    head, soft, deep = TONES[tone]
    ink = INK.get(tone, "#ffffff")
    title = html.escape(kicker)
    if tone in LABEL:
        title = f'<span style="background:{LABEL[tone]};padding:2px 10px;border-radius:6px">{title}</span>'
    badge = ENV_BADGE.get(env, "#475569")
    icon = (
        f'<img src="cid:{icon_cid}" width="56" height="56" alt="" style="display:block;border:0;border-radius:12px">'
        if icon_cid
        else ""
    )
    content = "".join(part(k, d, tone) for k, d in blocks(body))
    pre = html.escape(" ".join(body.split())[:140])
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light only"><meta name="supported-color-schemes" content="light only">
<title>{html.escape(subject)}</title></head>
<body style="margin:0;padding:0;background:#f3f1ec">
<div style="display:none;max-height:0;overflow:hidden;opacity:0">{pre}</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f3f1ec">
<tr><td align="center" style="padding:24px 12px">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:600px;background:#ffffff;
border-radius:16px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.08)">
<tr><td style="background:{head};padding:22px 24px">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
<td width="56" valign="middle" style="padding-right:16px">{icon}</td>
<td valign="middle">
<div style="font:600 11px/1.4 {FONT};letter-spacing:.12em;text-transform:uppercase;color:{ink};opacity:.85">
{html.escape(company)}&nbsp;&nbsp;<span style="display:inline-block;padding:2px 8px;border-radius:10px;background:{badge};
color:#ffffff;letter-spacing:.08em">{html.escape(env)}</span></div>
<div style="margin-top:6px;font:700 20px/1.3 {FONT};color:{ink}">{title}</div>
</td></tr></table></td></tr>
<tr><td style="padding:22px 24px 6px">
<p style="margin:0 0 16px;padding:10px 14px;border-left:4px solid {ACCENT.get(tone, head)};background:{soft};border-radius:6px;
font:600 15px/1.45 {FONT};color:{deep}">{html.escape(subject)}</p>
{content}
</td></tr>
<tr><td style="padding:14px 24px 20px;border-top:1px solid #eef0f3;font:12px/1.5 {FONT};color:#6b7280">
{html.escape(footer)}<br>Lakehouse platform</td></tr>
</table></td></tr></table></body></html>"""


def build(subject, body, *, tone, kicker, company, env, footer):
    """EmailMessage: plain text (body + footer, as before) with an HTML alternative and the inline icon.
    The caller sets Subject prefix, From and To exactly as before; Subject here is the plain subject line."""
    if tone not in TONES:
        raise ValueError(f"tone must be one of {sorted(TONES)}")
    msg = EmailMessage()
    msg.set_content(body + "\n\n-- " + footer)
    gif = ASSETS / f"{tone}.gif"
    cid = make_msgid(domain="lakehouse.local")[1:-1] if gif.exists() else None
    msg.add_alternative(
        render(
            subject,
            body,
            tone=tone,
            kicker=kicker,
            company=company,
            env=env,
            footer=footer,
            icon_cid=cid,
        ),
        subtype="html",
    )
    if cid:
        msg.get_payload()[1].add_related(
            gif.read_bytes(),
            "image",
            "gif",
            cid=f"<{cid}>",
            filename=f"{tone}.gif",
            disposition="inline",
        )
    return msg
