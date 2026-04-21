"""Tests for the built-in data resolver."""
from backend.services.resolver import Provenance, ResolvedInputs


def test_provenance_dataclass_shape():
    p = Provenance(
        concentration={"grain": "state", "source": "epa_aqs"},
        population={"grain": "tract", "source": "acs"},
        incidence={"grain": "national", "source": "crf_default"},
    )
    assert p.concentration["grain"] == "state"
    assert p.population["source"] == "acs"
    assert p.incidence["grain"] == "national"


def test_resolved_inputs_dataclass_shape():
    import numpy as np
    r = ResolvedInputs(
        zone_ids=["06001", "06003"],
        zone_names=["Alameda County", "Alpine County"],
        parent_ids=["06", "06"],
        geometries=[{"type": "Polygon", "coordinates": []}] * 2,
        c_baseline=np.array([12.5, 8.0]),
        c_control=np.array([5.0, 5.0]),
        population=np.array([1_600_000, 1_100]),
        provenance=Provenance(
            concentration={"grain": "state", "source": "epa_aqs"},
            population={"grain": "tract", "source": "acs"},
            incidence={"grain": "national", "source": "crf_default"},
        ),
        warnings=[],
    )
    assert len(r.zone_ids) == 2
    assert r.population.sum() == 1_601_100
