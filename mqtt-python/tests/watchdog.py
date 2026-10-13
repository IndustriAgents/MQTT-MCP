"""Run a command and stop it when this process's stdin closes.

Usage: python watchdog.py COMMAND [ARGS...]

The test fixtures start helper processes (a mosquitto broker) through this
script with stdin connected to a pipe from pytest. If pytest dies, even with
SIGKILL, the pipe closes, and the helper is terminated instead of being left
behind. SIGTERM/SIGINT to the watchdog are passed on to the command.
"""

import os
import signal
import subprocess
import sys
import threading


def main() -> int:
    child = subprocess.Popen(sys.argv[1:], stdin=subprocess.DEVNULL)

    def stop(*_args) -> None:
        if child.poll() is None:
            child.terminate()
            try:
                child.wait(5)
            except subprocess.TimeoutExpired:
                child.kill()

    def watch_stdin() -> None:
        try:
            # os.read, not sys.stdin: a daemon thread blocked inside the buffered
            # reader makes the interpreter abort at exit.
            while os.read(sys.stdin.fileno(), 4096):
                pass
        except (OSError, ValueError):
            pass
        stop()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    threading.Thread(target=watch_stdin, daemon=True).start()
    return child.wait()


if __name__ == "__main__":
    sys.exit(main())
