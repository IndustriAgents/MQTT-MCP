"""Compatibility module for the composed server and its existing entry point."""

import sys

from . import server as _server

sys.modules[__name__] = _server
