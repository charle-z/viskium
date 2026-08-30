"""Small defensive ceilings for Viskium's control-plane inputs.

These are safety limits for the dependency-free foundation, not calibrated
camera or model budgets. Product budgets remain gated on hardware profiling.
"""

from __future__ import annotations

MAX_CONFIG_FILE_BYTES = 1_048_576
MAX_SYNTHETIC_REPLAY_FRAMES = 10_000

__all__ = ["MAX_CONFIG_FILE_BYTES", "MAX_SYNTHETIC_REPLAY_FRAMES"]
