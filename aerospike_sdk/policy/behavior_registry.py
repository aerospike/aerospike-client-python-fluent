# Copyright 2025-2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""Global registry for Behavior instances, enabling name-based lookup."""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Optional

if TYPE_CHECKING:
    from aerospike_sdk.policy.behavior import Behavior

_registry: Dict[str, Behavior] = {}


def _register(behavior: Behavior) -> None:
    """Register a behavior (called automatically by Behavior.__init__)."""
    _registry[behavior.name] = behavior


def get_behavior(name: str) -> Optional[Behavior]:
    """Look up a registered Behavior by name, or ``None`` if not found."""
    return _registry.get(name)


def get_behavior_or_default(name: str) -> Behavior:
    """Look up a registered Behavior by name, falling back to DEFAULT."""
    b = _registry.get(name)
    if b is not None:
        return b
    default = _registry.get("DEFAULT")
    if default is None:
        raise RuntimeError("Behavior.DEFAULT has not been created yet")
    return default


def get_all_behaviors() -> Dict[str, Behavior]:
    """Return a snapshot of all registered behaviors."""
    return dict(_registry)
