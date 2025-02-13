"""Definition of a minimal demo plugin-implementation."""

from typing import Optional
import sys
from dataclasses import dataclass, field
from random import randint

from dcm_common.logger import LoggingContext as Context
from .interface import PluginInterface, PluginResult
from .types import (
    FreeFormSignature,
    Argument,
    JSONType,
    PluginExecutionContext,
    Dependency,
    PythonDependency,
)


@dataclass
class DemoPluginResult(PluginResult):
    """
    Data model for the result of `DemoPlugin`-invocations.
    """

    success: Optional[bool] = None


@dataclass
class DemoPluginContext(PluginExecutionContext):
    """
    Data model for the execution context of `DemoPlugin`-invocations.
    """

    result: DemoPluginResult = field(default_factory=DemoPluginResult)


class DemoPlugin(PluginInterface):
    """
    Implementation of a minimal plugin for testing purposes.
    """

    _NAME = "demo-plugin"
    _DISPLAY_NAME = "Demo Plugin"
    _DESCRIPTION = "Some plugin description"
    _CONTEXT = "testing"
    _DEPENDENCIES = [
        PythonDependency("dcm-common"),
        Dependency("python", sys.version),
    ]
    _SIGNATURE = FreeFormSignature(
        success=Argument(
            type_=JSONType.BOOLEAN,
            required=False,
            description=(
                "whether the result should be flagged as successful (mutually "
                + "exclusive with 'success_rate')"
            ),
            example=True,
        ),
        success_rate=Argument(
            type_=JSONType.INTEGER,
            required=False,
            description=(
                "rate with which results are flagged as successful (mutually "
                + "exclusive with 'success')"
            ),
            example=50,
        ),
    )
    _RESULT_TYPE = DemoPluginResult

    @classmethod
    def _validate_more(cls, kwargs):
        if "success" not in kwargs and "success_rate" not in kwargs:
            return False, "missing both 'success' and 'success_rate' in args"
        if "success" in kwargs and "success_rate" in kwargs:
            return False, "got both 'success' and 'success_rate' in args"
        if "success_rate" in kwargs and not 0 <= kwargs["success_rate"] <= 100:
            return False, "bad value for 'success_rate'"
        return True, ""

    def _eval(self, **kwargs) -> bool:
        """Runs plugin-logic and returns result."""
        if "success" in kwargs:
            return kwargs["success"]
        return kwargs["success_rate"] >= randint(1, 100)

    def _get(
        self, context: DemoPluginContext, /, **kwargs
    ) -> DemoPluginResult:
        context.set_progress("evaluating data")
        context.push()
        context.result.success = self._eval(**kwargs)
        if not context.result.success:
            context.result.log.log(Context.ERROR, body="Not successful..")

        return context.result

    def get(  # this simply narrows down the involved types
        self, context: Optional[DemoPluginContext], /, **kwargs
    ) -> DemoPluginResult:
        return super().get(context, **kwargs)
