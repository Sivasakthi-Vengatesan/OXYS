"""
Generate pixel art SVG matching Image 1 mascot sprite.
"""
import os

# Matrix 28 columns x 24 rows
# Legend:
# . = transparent
# X = dark outline (#35293d)
# D = dark connector fill (#4e4157)
# L = light connector highlight (#6e5e7a)
# B = body lavender fill (#c8b6e2)
# H = body highlight lavender (#dfd2ed)
# S = body shadow lavender (#b39fcc)
# E = dark eyes (#231c2a)

SPRITE_MAP = [
    "........XX..........XX........",
    "........XX..........XX........",
    ".......XDDX........XDDX.......",
    ".......XLLX........XLLX.......",
    ".....XXXBBXXXXXXXXXXBBXXX.....",
    "....XDDXBBBBBBBBBBBBBBXDDX....",
    "....XLLXBBBBBBBBBBBBBBXLLX....",
    "...XXBBXBBBBBBBBBBBBBBXBBXX...",
    "..XBBXBXBBBBBBBBBBBBBBXBXBBX..",
    "..XBBXBXBBBBEEBBEEBBBBXBXBBX..",
    "..XBBXBXBBBBEEBBEEBBBBXBXBBX..",
    "..XBBXBXBBBBEEBBEEBBBBXBXBBX..",
    "..XLLXBXBBBBBBBBBBBBBBXBXLLX..",
    "...XXBBXBBBBBBBBBBBBBBXBBXX...",
    "....XBBBBBBBBBBBBBBBBBBBBX....",
    ".....XXXXBBBBXXXXBBBBXXXX.....",
    "........XBBBX....XBBBX........",
    "........XBBBX....XBBBX........",
    "........XBBBX....XBBBX........",
    ".......XDDDDX....XDDDDX.......",
    ".......XDDDDX....XDDDDX.......",
    ".......XDDDDX....XDDDDX.......",
    "........XXXX......XXXX........"
]

COLOR_PALETTE = {
    "X": "#35293d",  # Deep dark purple border
    "D": "#4a3c54",  # Dark connector plug
    "L": "#6a5775",  # Light connector plug
    "B": "#cbb7e2",  # Body lavender
    "H": "#ded3ed",  # Body highlight
    "S": "#b49ecb",  # Body shadow
    "E": "#231c2a"   # Black pixel eyes
}

def generate_svg(output_path):
    rows = len(SPRITE_MAP)
    cols = len(SPRITE_MAP[0])
    
    rects = []
    for y, row in enumerate(SPRITE_MAP):
        for x, char in enumerate(row):
            if char in COLOR_PALETTE:
                color = COLOR_PALETTE[char]
                rects.append(f'<rect x="{x}" y="{y}" width="1" height="1" fill="{color}"/>')
                
    svg_content = f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {cols} {rows}" width="100%" height="100%" shape-rendering="crispEdges">
  <g>
    {''.join(rects)}
  </g>
</svg>'''
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(svg_content)
    print(f"Generated SVG at {output_path}")

if __name__ == "__main__":
    generate_svg("assets/oxys-mascot.svg")
