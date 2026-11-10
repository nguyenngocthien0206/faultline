from collections.abc import Callable
from pathlib import Path

import pytest
from datagen_helpers import Rows, read_dataset

from faultline.datagen import DatasetSpec, generate


@pytest.fixture(scope="session")
def tiny_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The tiny profile, generated once per test session."""
    root = tmp_path_factory.mktemp("tiny")
    generate(DatasetSpec.from_profile("tiny"), root, workers=1)
    return root


@pytest.fixture(scope="session")
def landing(tiny_root: Path) -> Callable[[str, str], Rows]:
    return lambda source, dataset: read_dataset(tiny_root, source, dataset)
