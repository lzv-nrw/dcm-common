from dcm_common.plugins import PluginInterface
from dcm_common.plugins.types import FreeFormSignature


class ExternalPlugin(PluginInterface):
    _NAME = "a-plugin"
    _DISPLAY_NAME = "A Plugin"
    _DESCRIPTION = ""
    _CONTEXT = "testing"
    _SIGNATURE = FreeFormSignature()

    def _get(self, context, /, **kwargs):
        return context.result
