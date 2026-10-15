from .config import Settings, load_settings, merge_env_file
from .domain.model import logger

_settings: Settings | None = None


def get_settings(env_file: str | None = None) -> Settings:
    """Load settings once, after merging a .env file into the environment.

    Variables already set in the environment take precedence. ``env_file`` is
    the explicit ``--env-file``; without it only ``mqtt-python/.env`` of a source
    checkout is read (never the working directory), and only for MQTT_* and
    SPARKPLUG_* variables other than MQTT_WRITES_ENABLED (warnings on stderr say
    what was ignored).
    """
    global _settings
    if _settings is None:
        for warning in merge_env_file(env_file):
            logger.warning(warning)
        _settings = load_settings()
    return _settings


def set_settings(settings: Settings | None) -> None:
    """Use these settings instead of the environment (tests, embedding)."""
    global _settings
    _settings = settings
