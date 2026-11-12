import pytest

from faultline.connectors.s3 import LandingBucket
from faultline.connectors.workspace import UnsafeWorkspaceError


def _bucket() -> LandingBucket:
    # Port 9 (discard) on localhost: any request that slipped through would fail.
    return LandingBucket("http://127.0.0.1:9", access_key="key", secret_key="secret")


def test_building_a_bucket_opens_nothing() -> None:
    bucket = _bucket()

    assert bucket._client is None


@pytest.mark.parametrize("name", ["", "landing", "fl_xyz", "fl_0123abcd/..", "../fl_0123abcd"])
def test_deleting_refuses_anything_but_a_workspace(name: str) -> None:
    with pytest.raises(UnsafeWorkspaceError):
        _bucket().delete_workspace(name)
