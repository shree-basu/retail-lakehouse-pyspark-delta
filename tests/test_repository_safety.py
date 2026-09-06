from scripts.check_repo_safety import violations


def test_repository_automation_cannot_authenticate_or_deploy() -> None:
    assert violations() == []
