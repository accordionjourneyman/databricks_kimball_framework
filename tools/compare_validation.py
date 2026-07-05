"""Quick comparison of validation optimization results."""

import glob
import json

results = []
for pattern in [
    "validation_full_*.json",
    "validation_state_aware_*.json",
    "validation_approximate_*.json",
    "validation_combined_*.json",
]:
    for f in sorted(glob.glob(f"tools/benchmarks/results/{pattern}")):
        with open(f) as fp:
            results.append(json.load(fp))
results.sort(key=lambda r: (r["scenario"], r["scale"]))

print(f"{'Scenario':<25} {'Scale':<8} {'1st(ms)':>8} {'2nd(ms)':>8}")
print("-" * 55)
for r in results:
    print(
        f"{r['scenario']:<25} {r['scale']:<8} {r['first_ms']:>8.0f} {r['second_ms']:>8.0f}"
    )

print()
print("SPEEDUP vs full validation (2nd run):")
for scale in ["tiny", "small"]:
    full = next(
        (
            r
            for r in results
            if r["scenario"] == "validation_full" and r["scale"] == scale
        ),
        None,
    )
    if not full:
        continue
    print(f"  {scale}:")
    for opt in ["state_aware", "approximate", "combined"]:
        opt_r = next(
            (
                r
                for r in results
                if r["scenario"] == f"validation_{opt}" and r["scale"] == scale
            ),
            None,
        )
        if opt_r:
            speedup = (full["second_ms"] - opt_r["second_ms"]) / full["second_ms"] * 100
            print(
                f"    {opt}: {speedup:+.1f}% ({opt_r['second_ms']:.0f}ms vs {full['second_ms']:.0f}ms)"
            )
