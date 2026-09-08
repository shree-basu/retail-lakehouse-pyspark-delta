"""Fail CI if repository automation gains authentication or deployment capability."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ROOT = ROOT / ".github" / "workflows"
FORBIDDEN_WORKFLOW_PATTERNS = {
    r"id-token:\s*write": "OIDC token permission",
    r"\bsecrets\.": "GitHub secret access",
    r"google-github-actions/auth": "Google Cloud authentication",
    r"databricks(?:\s+bundle)?\s+deploy": "Databricks deployment",
    r"\bgcloud\b": "Google Cloud CLI",
    r"\bterraform\s+apply\b": "Terraform apply",
    r"\bdbt\s+(?:run|build)\b": "authenticated dbt execution",
    r"workflow_dispatch": "manual workflow execution",
}


def violations() -> list[str]:
    findings: list[str] = []
    for path in sorted(WORKFLOW_ROOT.glob("*.y*ml")):
        text = path.read_text(encoding="utf-8")
        for pattern, label in FORBIDDEN_WORKFLOW_PATTERNS.items():
            if re.search(pattern, text, flags=re.IGNORECASE):
                findings.append(f"{path.relative_to(ROOT)}: {label}")
    profile = (ROOT / ".github" / "dbt" / "profiles.yml").read_text(encoding="utf-8")
    if "host: https://example.invalid" not in profile or "token: not-a-real-token" not in profile:
        findings.append(".github/dbt/profiles.yml: CI profile is not the inert placeholder")
    return findings


def main() -> None:
    findings = violations()
    if findings:
        raise SystemExit("Unsafe repository automation:\n" + "\n".join(findings))
    print("Repository automation is cloud-free and deployment-disabled.")


if __name__ == "__main__":
    main()
