from dcm_common.plugins import PluginInterface
from dcm_common.plugins.types import FreeFormSignature


class BPlugin(PluginInterface):
    _NAME = "b-plugin"
    _DISPLAY_NAME = "B Plugin"
    _DESCRIPTION = ""
    _CONTEXT = "testing2"
    _SIGNATURE = FreeFormSignature()

    def _get(self, context, /, **kwargs):
        return context.result


class ExternalPlugin(BPlugin):
    pass
