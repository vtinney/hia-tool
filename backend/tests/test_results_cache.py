from backend.services.results_cache import save_result, load_result, ResultNotFound
import pytest


class FakeResponse:
    def __init__(self, x): self.x = x
    def model_dump(self): return {"x": self.x}


def test_save_and_load_roundtrip():
    save_result("abc123", FakeResponse(7))
    loaded = load_result("abc123")
    assert loaded == {"x": 7}


def test_load_missing_raises():
    with pytest.raises(ResultNotFound):
        load_result("nonexistent-id")
