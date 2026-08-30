## Summary

<!-- One or two sentences describing the change. -->

## Type of change

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that would cause existing
      functionality to not work as expected)
- [ ] Documentation update
- [ ] Refactor (no functional change)
- [ ] Performance improvement
- [ ] Test improvement

## Production-readiness checklist

<!-- All items must be true before merge. -->

- [ ] `pytest tests/unit/` passes locally
- [ ] `ruff check src/ tests/` passes
- [ ] `mypy src/kimball/common/ src/kimball/cli.py` passes
- [ ] `CHANGELOG.md` updated under `## Unreleased`
- [ ] Docs updated if public API changed (`docs/CONFIGURATION.md`,
      `README.md`, or relevant doc page)
- [ ] Tests added for new behavior or regression coverage
- [ ] No secrets, tokens, or hardcoded credentials in diff
- [ ] Backward compatibility considered (or breaking change
      explicitly justified in the description below)

## Related issues

<!-- Link issues with "Fixes #123" or "Relates to #456". -->

## Test plan

<!-- How did you verify the change works? Include commands, configs,
     or manual steps. -->

## Breaking change notes

<!-- If you checked "Breaking change" above, describe the migration
     path and whether a deprecation warning was added. -->
