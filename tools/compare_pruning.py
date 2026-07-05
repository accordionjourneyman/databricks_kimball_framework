"""Compare pruning benchmark results."""
import json
import glob

results = []
for pattern in ["pruning_baseline_*.json", "pruning_adaptive_*.json"]:
    for f in sorted(glob.glob(f"tools/benchmarks/results/{pattern}")):
        with open(f) as fp:
            results.append(json.load(fp))
results.sort(key=lambda r: (r["scenario"], r["scale"]))

print(f"{'Scenario':<25} {'Scale':<8} {'1st(ms)':>8} {'2nd(ms)':>8}")
print("-" * 55)
for r in results:
    print(f"{r['scenario']:<25} {r['scale']:<8} {r['first_ms']:>8.0f} {r['second_ms']:>8.0f}")

print()
print("SPEEDUP from adaptive pruning:")
for scale in ["tiny", "small"]:
    base = next(
        (r for r in results if r["scenario"] == "pruning_baseline" and r["scale"] == scale),
        None,
    )
    adapt = next(
        (r for r in results if r["scenario"] == "pruning_adaptive" and r["scale"] == scale),
        None,
    )
    if base and adapt:
        first_speedup = (base["first_ms"] - adapt["first_ms"]) / base["first_ms"] * 100
        second_speedup = (base["second_ms"] - adapt["second_ms"]) / base["second_ms"] * 100
        print(f"  {scale}:")
        print(f"    1st run: {first_speedup:+.1f}% ({base['first_ms']:.0f}ms -> {adapt['first_ms']:.0f}ms)")
        print(f"    2nd run: {second_speedup:+.1f}% ({base['second_ms']:.0f}ms -> {adapt['second_ms']:.0f}ms)")
        print(f"    Adaptive result: tracked_in_target={adapt.get('tracked_columns_in_target')}, extras_in_target={adapt.get('extra_columns_in_target')}")
