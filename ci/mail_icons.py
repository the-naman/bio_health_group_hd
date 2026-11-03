"""Animated status icons for the HTML mails (28d, same file in both repos).

Writes ci/mail_assets/<tone>.gif (ok, warn, fail, info, urgent): 56 x 56 px, about 1.2 s loop, drawn on the tone colour so they
need no transparency (Gmail, Outlook and phone apps all play animated GIFs; none play CSS animation). The GIFs are
committed; run this only to change their look:
    pip install pillow==11.0.0 && python ci/mail_icons.py
Deterministic: the same code always writes the same bytes.
"""

import math
from pathlib import Path

from PIL import Image, ImageDraw

TONES = {
    "ok": "#1f8a4c",
    "warn": "#b7791f",
    "fail": "#c0392b",
    "info": "#2f6fde",
    "urgent": "#f2c230",
}
DARK = (165, 14, 14)  # urgent: red disc with a yellow "!" on the yellow header
SIZE, SCALE, FRAMES, MS = 56, 4, 24, 50
OUT = Path(__file__).resolve().parent / "mail_assets"


def rgb(h):
    return tuple(int(h[i : i + 2], 16) for i in (1, 3, 5))


def mix(a, b, t):
    return tuple(round(x + (y - x) * t) for x, y in zip(a, b))


def frame(tone, i):
    bg = rgb(TONES[tone])
    white = DARK if tone == "urgent" else (255, 255, 255)
    s = SIZE * SCALE
    im = Image.new("RGB", (s, s), bg)
    d = ImageDraw.Draw(im)
    c = s / 2
    t = i / FRAMES
    # expanding ring: a soft "pulse" that fades as it grows
    r = s * (0.26 + 0.2 * t)
    d.ellipse(
        [c - r, c - r, c + r, c + r],
        outline=mix(bg, white, 0.55 * (1 - t)),
        width=3 * SCALE,
    )
    # solid disc with a gentle breathing size
    r0 = s * (0.25 + 0.012 * math.sin(2 * math.pi * t))
    d.ellipse([c - r0, c - r0, c + r0, c + r0], fill=white)
    w = round(4.2 * SCALE)
    if tone == "ok":  # tick
        d.line(
            [
                (c - s * 0.11, c + s * 0.0),
                (c - s * 0.025, c + s * 0.085),
                (c + s * 0.12, c - s * 0.075),
            ],
            fill=bg,
            width=w,
            joint="curve",
        )
    elif tone in ("warn", "fail", "urgent"):  # exclamation / cross
        if tone in ("warn", "urgent"):
            d.line([(c, c - s * 0.11), (c, c + s * 0.035)], fill=bg, width=w)
            d.ellipse(
                [
                    c - w * 0.6,
                    c + s * 0.085 - w * 0.6,
                    c + w * 0.6,
                    c + s * 0.085 + w * 0.6,
                ],
                fill=bg,
            )
        else:
            k = s * 0.085
            d.line([(c - k, c - k), (c + k, c + k)], fill=bg, width=w)
            d.line([(c - k, c + k), (c + k, c - k)], fill=bg, width=w)
    else:  # info: "i"
        d.ellipse(
            [c - w * 0.6, c - s * 0.1 - w * 0.6, c + w * 0.6, c - s * 0.1 + w * 0.6],
            fill=bg,
        )
        d.line([(c, c - s * 0.035), (c, c + s * 0.11)], fill=bg, width=w)
    return im.resize((SIZE, SIZE), Image.LANCZOS).convert("P", palette=Image.ADAPTIVE, colors=64)


def main():
    OUT.mkdir(exist_ok=True)
    for tone in TONES:
        frames = [frame(tone, i) for i in range(FRAMES)]
        path = OUT / f"{tone}.gif"
        frames[0].save(
            path,
            save_all=True,
            append_images=frames[1:],
            duration=MS,
            loop=0,
            optimize=True,
            disposal=1,
        )
        print(f"{path.name}: {path.stat().st_size} bytes")


if __name__ == "__main__":
    main()
