"""Compare radon metrics between main tree and worktree."""
import json, os, sys

# Use radon's Python API directly
from radon.complexity import cc_visit
from radon.visitors import ComplexityVisitor

REPO = r"C:\Users\Tiago Marques\Documents\databricks_kimball_framework"
WORKTREE = os.path.join(REPO, ".worktrees", "simplify-core")


def get_complexity(path):
    results = {}
    src_dir = os.path.join(path, "src", "kimball")
    for root, dirs, files in os.walk(src_dir):
        for f in files:
            if not f.endswith(".py"):
                continue
            fpath = os.path.join(root, f)
            with open(fpath, encoding="utf-8") as fh:
                try:
                    source = fh.read()
                except Exception:
                    continue
            try:
                visitor = ComplexityVisitor.from_code(source)
                rel = os.path.relpath(fpath, src_dir).replace("\\", "/")
                results[rel] = []
                for block in visitor.blocks:
                    results[rel].append({
                        "name": block.name,
                        "classname": getattr(block, "class", None) or "",
                        "lineno": block.lineno,
                        "complexity": block.complexity,
                        "rank": _rank(block.complexity),
                    })
            except Exception as e:
                print(f"  Skipping {f}: {e}", file=sys.stderr)
    return results


def _rank(c):
    if c <= 5: return "A"
    if c <= 10: return "B"
    if c <= 20: return "C"
    if c <= 30: return "D"
    if c <= 40: return "E"
    return "F"


def grade_dist(data):
    grades = {"A": 0, "B": 0, "C": 0, "D": 0, "E": 0, "F": 0}
    for funcs in data.values():
        for f in funcs:
            grades[f["rank"]] = grades.get(f["rank"], 0) + 1
    return grades


def hotspots(data, label):
    print(f"\n=== HOTSPOTS (D/E/F) -- {label} ===")
    for fname, funcs in data.items():
        for f in funcs:
            if f["rank"] in "DEF":
                name = f.get("name", "?")
                cls = f.get("classname", "")
                full = f"{cls}.{name}" if cls else name
                print(f"  {f['rank']} ({f['complexity']}) {full} @ {fname}:{f['lineno']}")


def count_lines(path):
    total = 0
    for root, dirs, files in os.walk(os.path.join(path, "src", "kimball")):
        for f in files:
            if f.endswith(".py"):
                with open(os.path.join(root, f), encoding="utf-8") as fh:
                    total += sum(1 for _ in fh)
    return total


print("Analyzing main tree...")
d1 = get_complexity(REPO)
print("Analyzing worktree...")
d2 = get_complexity(WORKTREE)

g1 = grade_dist(d1)
g2 = grade_dist(d2)
t1 = sum(g1.values())
t2 = sum(g2.values())

print("\n=== COMPLEXITY GRADE DISTRIBUTION ===")
print(f"{'Grade':>6} {'Main':>8} {'Worktree':>10} {'Delta':>8}")
for g in "ABCDEF":
    a = g1.get(g, 0)
    b = g2.get(g, 0)
    print(f"{g:>6} {a:>8} {b:>10} {b - a:>+8}")
print(f"{'Total':>6} {t1:>8} {t2:>10} {t2 - t1:>+8}")

print(f"\n=== LINE COUNTS ===")
l1 = count_lines(REPO)
l2 = count_lines(WORKTREE)
print(f"Main tree:    {l1} lines")
print(f"Worktree:     {l2} lines")
print(f"Delta:        {l2 - l1:>+} lines ({((l2-l1)/l1)*100:+.1f}%)")

hotspots(d1, "MAIN TREE")
hotspots(d2, "WORKTREE")
