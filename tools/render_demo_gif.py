#!/usr/bin/env python3
"""Render assets/demo.cast to a GIF (or a single PNG frame) with pyte + Pillow.

The SVG (svg-term) is the primary asset; this is the GIF for places that don't
render SVG, and a way to eyeball a frame. Render-time deps only — not part of
the package:

    pip install pillow pyte
    python tools/render_demo_gif.py                     # -> assets/demo.gif
    python tools/render_demo_gif.py --at 22 frame.png   # one frame at t=22s

pyte replays the asciicast byte stream into a real screen buffer, so clears,
cursor moves, and SGR colors come out as they would in a terminal.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pyte
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parent.parent
CAST = ROOT / "assets" / "demo.cast"
FONT = r"C:\Windows\Fonts\consola.ttf"
FONT_B = r"C:\Windows\Fonts\consolab.ttf"
FONT_SYM = r"C:\Windows\Fonts\seguisym.ttf"   # ✓ and other dingbats Consolas lacks
FS = 16
BG = (13, 17, 23)
PAD = 16
BAR = 30
NAMED = {
    "default": (201, 209, 217), "white": (255, 255, 255),
    "green": (86, 211, 100), "blue": (88, 166, 255), "cyan": (57, 197, 207),
    "brown": (210, 153, 34), "yellow": (210, 153, 34), "red": (248, 81, 73),
    "magenta": (188, 140, 255), "black": (110, 118, 129),
}


def color(name):
    if name in NAMED:
        return NAMED[name]
    if isinstance(name, str) and len(name) == 6:
        try:
            return tuple(int(name[i:i + 2], 16) for i in (0, 2, 4))
        except ValueError:
            pass
    return NAMED["default"]


def load_events():
    lines = CAST.read_text(encoding="utf-8").splitlines()
    header = json.loads(lines[0])
    events = [json.loads(l) for l in lines[1:] if l.strip()]
    return header["width"], header["height"], events


def render(screen, font, font_b, font_sym, cw, ch):
    w = screen.columns * cw + 2 * PAD
    h = screen.lines * ch + 2 * PAD + BAR
    img = Image.new("RGB", (w, h), BG)
    d = ImageDraw.Draw(img)
    d.rectangle([0, 0, w, BAR], fill=(22, 27, 34))
    for i, c in enumerate(((255, 95, 88), (255, 189, 46), (39, 201, 63))):
        d.ellipse([PAD + i * 18 - 5, BAR // 2 - 5, PAD + i * 18 + 5, BAR // 2 + 5], fill=c)
    for row in range(screen.lines):
        line = screen.buffer[row]
        for col in range(screen.columns):
            ch_ = line[col]
            if ch_.data and ch_.data != " ":
                x = PAD + col * cw
                y = BAR + PAD + row * ch
                f = font_sym if ord(ch_.data[0]) >= 0x2700 else (font_b if ch_.bold else font)
                d.text((x, y), ch_.data, font=f, fill=color(ch_.fg))
    return img


def feed_until(stream, screen, events, idx, t):
    while idx < len(events) and events[idx][0] <= t:
        stream.feed(events[idx][2])
        idx += 1
    return idx


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--at", type=float)
    ap.add_argument("out", nargs="?", default=str(ROOT / "assets" / "demo.gif"))
    ap.add_argument("--fps", type=float, default=12.0)
    args = ap.parse_args()

    cols, rows, events = load_events()
    font = ImageFont.truetype(FONT, FS)
    font_b = ImageFont.truetype(FONT_B, FS)
    font_sym = ImageFont.truetype(FONT_SYM, FS)
    box = font.getbbox("M")
    cw, ch = box[2] - box[0] + 1, FS + 6

    screen = pyte.Screen(cols, rows)
    stream = pyte.Stream(screen)

    if args.at is not None:
        feed_until(stream, screen, events, 0, args.at)
        render(screen, font, font_b, font_sym, cw, ch).save(args.out)
        print(f"wrote {args.out} (frame at {args.at}s)")
        return

    end = events[-1][0]
    dt = 1.0 / args.fps
    frames, durations = [], []
    idx, t, last = 0, 0.0, None
    while t <= end:
        idx = feed_until(stream, screen, events, idx, t)
        snap = "\n".join(screen.display)
        if snap == last and frames:                 # collapse static holds
            durations[-1] += dt
        else:
            frames.append(render(screen, font, font_b, font_sym, cw, ch))
            durations.append(dt)
            last = snap
        t += dt

    frames[0].save(args.out, save_all=True, append_images=frames[1:], loop=0,
                   duration=[int(x * 1000) for x in durations], optimize=True, disposal=2)
    print(f"wrote {args.out}  ({len(frames)} frames, {end:.1f}s)")


if __name__ == "__main__":
    main()
