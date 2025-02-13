"""
Data-model definitions
"""

from typing import Optional
from dataclasses import dataclass, field

from dcm_common.models import DataModel, JSONObject, Report as BaseReport
from dcm_common.services.plugins import PluginConfig


@dataclass
class _ChildConfig(DataModel):
    host: str
    body: JSONObject
    timeout: Optional[float] = 10


@dataclass
class DemoConfig(DataModel):
    """
    DemoConfig `DataModel`

    Keyword arguments:
    duration -- job's run duration (excluding possible children)
    success -- job's success
    childrin
    """

    duration: Optional[float] = 1.0
    success: Optional[bool] = False
    success_plugin: Optional[PluginConfig] = None
    children: Optional[list[_ChildConfig]] = None

    @DataModel.serialization_handler("children")
    @classmethod
    def children_serialization(cls, value):
        """Handle serialization of `children`."""
        if value is None:
            DataModel.skip()
        return [c.json for c in value]

    @DataModel.deserialization_handler("children")
    @classmethod
    def children_deserialization(cls, value):
        """Handle deserialization of `children`."""
        if value is None:
            DataModel.skip()
        return [_ChildConfig.from_json(c) for c in value]


@dataclass
class DemoResult(DataModel):
    """
    DemoResult `DataModel`

    Keyword arguments:
    success -- overall success of the job
    """

    success: Optional[bool] = None


@dataclass
class Report(BaseReport):
    """
    Report `DataModel`
    """
    data: DemoResult = field(default_factory=DemoResult)
    children: Optional[JSONObject] = None

    @DataModel.serialization_handler("children")
    @classmethod
    def children_serialization(cls, value):
        """Handle serialization of `children`."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("children")
    @classmethod
    def children_deserialization(cls, value):
        """Handle deserialization of `children`."""
        if value is None:
            DataModel.skip()
        return value
