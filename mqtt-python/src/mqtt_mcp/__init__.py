"""MQTT + Sparkplug B MCP server."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("mqtt-mcp")
except PackageNotFoundError:  # running from a source tree without installing
    __version__ = "0+unknown"
