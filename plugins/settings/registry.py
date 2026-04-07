from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional

import discord

Handler = Callable[[discord.Interaction, Dict[str, Any]], Awaitable[Optional[dict]]]
StatusFn = Callable[[], str]


@dataclass(frozen=True)
class FeatureAction:
    action_id: str
    label: str
    description: str = ""
    style: str = "secondary"  # primary/secondary/success/danger
    row: int = 1


@dataclass(frozen=True)
class SettingFeature:
    feature_id: str
    label: str
    description: str

    # Category metadata is defined by the feature/subpack (not the hub).
    category: str
    category_description: str

    handler: Handler
    status: Optional[StatusFn] = None  # short line, e.g. "✅ Enabled"
    actions: Optional[list[FeatureAction]] = None


class SettingsRegistry:
    def __init__(self) -> None:
        self._features: Dict[str, SettingFeature] = {}

    def register(self, feature: SettingFeature) -> None:
        if feature.feature_id in self._features:
            raise ValueError(f"Duplicate feature_id: {feature.feature_id}")
        self._features[feature.feature_id] = feature

    def unregister(self, feature_id: str) -> None:
        self._features.pop(feature_id, None)

    def get(self, feature_id: str) -> Optional[SettingFeature]:
        return self._features.get(feature_id)

    def all(self) -> list[SettingFeature]:
        return list(self._features.values())
