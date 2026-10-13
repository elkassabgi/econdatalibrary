"""The two share cards (the picture a link to the econ site or to the portal shows): their SVG sources.

    python -B tools/social_cards/make_cards.py            writes econ.svg and portal.svg beside this file

THE CARDS CARRY NO HEADLINE FIGURE. A card is a still picture: it cannot show the "being recalculated" notice
that /v1/stats and the home pages show beside the totals, and a figure on it is wrong the day the census
changes. The cards of 2026-08-21 showed totals and a source count that the sites no longer state. So the tiles
are words the pages themselves use:
  econ    "Every series carries its license, provenance, and producer-first citation." (catalog/site/index.html)
  portal  the three libraries of its own description (portal/index.html: HF, Econ, IP).
The design is the family's (the hf card): the same ground, title, tagline, tile row and byline.

RENDERING (by hand, then the PNGs are committed): a 1200x630 screenshot of each SVG with a local browser, e.g.
    chrome --headless=new --hide-scrollbars --window-size=1200,630 --screenshot=<out.png> file:///<path>/econ.svg
The PNGs live at catalog/site/assets/og-image.png and portal/assets/og-image.png (served; the SVGs are not).
tests/test_share_cards.py pins the texts of the two SVGs and the size of the two PNGs.
"""
from __future__ import annotations

import os

HERE = os.path.dirname(os.path.abspath(__file__))

HEAD = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 630" width="1200" height="630">
  <defs>
    <linearGradient id="bg" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#1a2332"/>
      <stop offset="100%" stop-color="#243044"/>
    </linearGradient>
  </defs>
  <rect width="1200" height="630" fill="url(#bg)"/>
  <g stroke="#d4a843" stroke-width="1" opacity="0.1">
    <line x1="0" y1="100" x2="1200" y2="100"/>
    <line x1="0" y1="200" x2="1200" y2="200"/>
    <line x1="0" y1="300" x2="1200" y2="300"/>
    <line x1="0" y1="400" x2="1200" y2="400"/>
    <line x1="0" y1="500" x2="1200" y2="500"/>
  </g>
"""
SANS = "Inter, Arial, Helvetica, sans-serif"
MONO = "JetBrains Mono, Consolas, monospace"


def card(white: str, gold: str, tagline: str, tiles, labels, one_label, byline: str) -> str:
    """tiles: three gold words; labels: three grey words under them, or None with `one_label` under the row."""
    xs = (330, 600, 870)
    out = [HEAD]
    out.append(f'  <text x="600" y="230" font-family="Georgia, serif" font-size="72" font-weight="700" text-anchor="middle" '
               f'fill="#ffffff">{white} <tspan fill="#d4a843">{gold}</tspan></text>\n')
    out.append(f'  <text x="600" y="290" font-family="{SANS}" font-size="28" text-anchor="middle" '
               f'fill="rgba(255,255,255,0.7)">{tagline}</text>\n')
    out.append(f'  <g font-family="{MONO}" fill="#d4a843" font-size="40" font-weight="700" text-anchor="middle">\n')
    for x, t in zip(xs, tiles):
        out.append(f'    <text x="{x}" y="418">{t}</text>\n')
    out.append("  </g>\n")
    out.append(f'  <g font-family="{SANS}" fill="rgba(255,255,255,0.6)" font-size="17" text-anchor="middle">\n')
    if labels:
        for x, t in zip(xs, labels):
            out.append(f'    <text x="{x}" y="455">{t}</text>\n')
    else:
        out.append(f'    <text x="600" y="458" font-size="19" letter-spacing="3">{one_label}</text>\n')
    out.append("  </g>\n")
    out.append(f'  <text x="600" y="560" font-family="{SANS}" font-size="22" text-anchor="middle" '
               f'fill="rgba(255,255,255,0.5)">{byline}</text>\n</svg>\n')
    return "".join(out)


CARDS = {
    "econ.svg": card("Econ Data", "Library", "Free economic &amp; financial time series data",
                     ("LICENSE", "PROVENANCE", "CITATION"), None, "ON EVERY SERIES",
                     "econdatalibrary.com &#183; Ahmed Elkassabgi &#183; University of Central Arkansas"),
    "portal.svg": card("Elkassabgi", "Data", "One account. Every library.",
                       ("HF", "ECON", "IP"),
                       ("1-MINUTE U.S. EQUITIES", "ECONOMIC &amp; FINANCIAL DATA", "PATENTS &amp; INNOVATION"), None,
                       "elkassabgidata.com &#183; Ahmed Elkassabgi &#183; University of Central Arkansas"),
}


def main() -> int:
    for name, svg in CARDS.items():
        with open(os.path.join(HERE, name), "w", encoding="utf-8", newline="\n") as fh:
            fh.write(svg)
        print("wrote", name, len(svg), "bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
