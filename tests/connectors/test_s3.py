import pytest

from faultline.connectors.s3 import LandingBucket
from faultline.connectors.workspace import UnmarkedWorkspaceError, UnsafeWorkspaceError


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


@pytest.mark.parametrize("key", ["landing/x.parquet", "x.parquet", "fl_zz/pos/x.parquet"])
def test_deleting_a_key_outside_a_workspace_is_refused(key: str) -> None:
    with pytest.raises(UnsafeWorkspaceError):
        _bucket().delete(key)


def test_a_workspace_is_marked_by_an_object_at_the_root_of_its_prefix() -> None:
    assert LandingBucket.mark_key("fl_0123abcd") == "fl_0123abcd/.faultline-workspace"
    with pytest.raises(UnsafeWorkspaceError):
        LandingBucket.mark_key("landing")


def test_a_prefix_with_the_name_but_not_the_mark_is_never_deleted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bucket = _bucket()
    monkeypatch.setattr(bucket, "list", lambda prefix: [f"{prefix}pos/orders/x.parquet"])

    with pytest.raises(UnmarkedWorkspaceError, match="fl_0123abcd/: not marked"):
        bucket.delete_workspace("fl_0123abcd")  # port 9: a delete would have failed loudly
    assert not bucket.is_marked("fl_0123abcd")
