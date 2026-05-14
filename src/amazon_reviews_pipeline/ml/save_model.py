# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Model persistence helpers."""

from __future__ import annotations


def save_model(model, model_path: str) -> None:
    model.write().overwrite().save(model_path)
