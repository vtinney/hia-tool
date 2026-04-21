"""Built-in HIA data resolver.

Given a pollutant/country/year/analysisLevel, assembles per-polygon
arrays of concentration, population, and incidence aligned to the
requested reporting polygon. Handles finest-of-each-input logic:
broadcast coarser inputs, aggregate finer inputs.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np


@dataclass
class Provenance:
    """Records the native grain and source of each resolved input."""
    concentration: dict[str, Any]
    population: dict[str, Any]
    incidence: dict[str, Any]


@dataclass
class ResolvedInputs:
    """Per-polygon arrays + metadata returned by the resolver."""
    zone_ids: list[str]
    zone_names: list[str | None]
    parent_ids: list[str | None]
    geometries: list[dict]
    c_baseline: np.ndarray
    c_control: np.ndarray
    population: np.ndarray
    provenance: Provenance
    warnings: list[str] = field(default_factory=list)
