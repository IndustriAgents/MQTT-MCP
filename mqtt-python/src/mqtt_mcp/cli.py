#!/usr/bin/env python3
"""Command-line entry point of the MQTT MCP server (console script ``mqtt-mcp``)."""

from __future__ import annotations

import argparse
import sys

from . import __version__
from .config import ENV_VARS


def _parser() -> argparse.ArgumentParser:
    env_help = "\n".join(f"  {name:<26} {text}" for name, text in ENV_VARS.items())
    return argparse.ArgumentParser(
        prog="mqtt-mcp",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "MQTT + Sparkplug B MCP server. It speaks MCP (JSON-RPC) on stdin/stdout, so it is"
            " normally\nlaunched by an MCP client; started by hand it waits silently for a"
            " client on stdin.\nRead-only unless MQTT_WRITES_ENABLED=true."
        ),
        epilog=(
            "configuration (environment variables, which win over files; --env-file PATH\n"
            "loads a file first; without it, only mqtt-python/.env of a source checkout is\n"
            "loaded, never the working directory, and only for MQTT_*/SPARKPLUG_* settings\n"
            "other than MQTT_WRITES_ENABLED, which counts only from the real environment\n"
            "or --env-file):\n"
            f"{env_help}"
        ),
    )


def main(argv: list[str] | None = None) -> None:
    parser = _parser()
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--env-file",
        metavar="PATH",
        help="load settings from this file instead of mqtt-python/.env; unlike that"
        " file, it may set MQTT_WRITES_ENABLED",
    )
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    from .mqtt_server import main as run_server

    run_server(env_file=args.env_file)


if __name__ == "__main__":
    main()
