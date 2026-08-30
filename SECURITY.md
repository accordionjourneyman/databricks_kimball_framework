# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.2.x   | :white_check_mark: |
| 0.1.x   | :x:                |
| < 0.1   | :x:                |

## Reporting a Vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**

Report security issues to: **t.diogo.marques@gmail.com**

You should receive an acknowledgment within 48 hours. If you do not,
follow up via the same email address.

### What to include

- Description of the vulnerability
- Steps to reproduce
- Affected versions
- Potential impact (data exposure, code execution, etc.)
- Any known mitigations

### What to expect

- **Acknowledgment:** within 48 hours
- **Initial assessment:** within 7 days
- **Patch timeline:**
  - Critical (RCE, data breach): within 30 days
  - High (data corruption, auth bypass): within 60 days
  - Medium/ Low: next minor release
- **Disclosure:** coordinated with the reporter. We will not disclose
  publicly until a patch is available, or 90 days have passed,
  whichever comes first.

## Security Design

The framework handles sensitive data (PII, secrets, credentials).
Key security properties:

- **Secrets:** resolved through `SecretResolver` with injected
  `environ` and `dbutils`. No key material is logged.
  See `src/kimball/common/secrets.py`.
- **PII:** tokenized via declarative config, not ad-hoc code.
  Tokenization strategy is enforced at pipeline runtime.
- **Manifests:** explicitly secret-free. The planner emits
  configuration, never credentials.
- **Dependencies:** scanned weekly via `pip-audit` in CI
  (`.github/workflows/security.yml`).
- **Static analysis:** CodeQL runs on every PR.
- **Secret scanning:** Gitleaks runs on every PR.

## Out of Scope

- Vulnerabilities in PySpark, Delta Lake, or Databricks Runtime
  (report to upstream maintainers)
- Vulnerabilities in user-written `transformation_sql`
  (the framework executes what you write)
- Denial of service via large inputs (resource limits are the
  operator's responsibility)
