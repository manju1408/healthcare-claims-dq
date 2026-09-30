import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(scope="session")
def spark():
    from claims.spark import get_spark

    s = get_spark("claims-tests")
    yield s
    s.stop()
