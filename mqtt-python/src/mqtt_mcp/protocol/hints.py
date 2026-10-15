from mcp.types import ToolAnnotations

from ..domain.model import METRICS_HELP

_READ = ToolAnnotations(readOnlyHint=True, openWorldHint=True)
_LOCAL = ToolAnnotations(readOnlyHint=True, openWorldHint=False)
_WRITE = ToolAnnotations(
    readOnlyHint=False, destructiveHint=True, idempotentHint=False, openWorldHint=True
)


def _metrics_doc(fn):
    """Put METRICS_HELP into the docstring where it says {METRICS_HELP}."""
    if fn.__doc__:
        fn.__doc__ = fn.__doc__.replace("{METRICS_HELP}", METRICS_HELP)
    return fn
