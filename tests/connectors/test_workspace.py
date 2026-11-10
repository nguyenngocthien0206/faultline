import pytest

from faultline.connectors.workspace import (
    UnsafeWorkspaceError,
    new_workspace_name,
    validate_workspace_name,
)


def test_new_names_are_valid_and_unique() -> None:
    names = {new_workspace_name() for _ in range(100)}

    assert len(names) == 100
    assert all(validate_workspace_name(n) == n for n in names)


@pytest.mark.parametrize(
    "name",
    [
        "default",
        "system",
        "public",
        "fl_",
        "fl_1234567",  # too short
        "fl_123456789",  # too long
        "fl_ABCDEF12",  # upper case
        "fl_abcd1234; DROP DATABASE default",
        " fl_abcd1234",
        "FL_abcd1234",
    ],
)
def test_anything_else_is_refused(name: str) -> None:
    with pytest.raises(UnsafeWorkspaceError):
        validate_workspace_name(name)
