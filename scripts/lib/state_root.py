"""Where broca keeps its durable state, resolved by the same rule the daemon uses.

The daemon's rule lives in crates/broca-module-serve/src/state_root.rs. In order:

  1. BROCA_STATE_ROOT, when set and non-empty, exactly as given.
  2. $XDG_DATA_HOME/cortexkit/broca, when XDG_DATA_HOME is set and non-empty.
  3. On Windows, $APPDATA/cortexkit/broca or $USERPROFILE/AppData/Roaming/cortexkit/broca.
  4. $HOME/.local/share/cortexkit/broca.

There is no fallback to a guessed relative path. With none of those set the daemon refuses to start rather than use a
relative path, and so does this helper: a script that guessed would read or sweep a tree the daemon
is not using and report a clean result about the wrong machine state.

Every operator script that locates the state root goes through default_state_root() so there is one
copy of the rule on this side too. scripts/test-state-root.py checks that no script spells the old
hard-coded path any more, and that the shell reader in wal-floor.sh agrees with this one.

The liveness probe passes the daemon's environment and platform explicitly; it must not
substitute the maintenance tool's home directory for the daemon's.

Operator config uses default_config_home(), a separate ladder matching the supervisor's
config resolver: XDG_CONFIG_HOME, Windows APPDATA or USERPROFILE/AppData/Roaming, then
HOME/.config. Configured relative paths are honoured; an all-unset environment is not guessed.
"""

from __future__ import annotations

import os
import pathlib
import sys
from typing import Mapping, Optional

STATE_ROOT_ENV = "BROCA_STATE_ROOT"


class StateRootError(RuntimeError):
    """No state root can be resolved from the environment."""


class ConfigHomeError(RuntimeError):
    """No config home can be resolved from the environment."""


def default_config_home(env: Optional[Mapping[str, str]] = None, *, platform: str = sys.platform) -> pathlib.Path:
    """Broca's config directory, including cortexkit/broca, without filesystem access.

    Unlike durable data, configured relative config paths are honoured. Unlike the supervisor,
    an all-unset environment raises rather than selecting a relative .config directory.
    """
    env = os.environ if env is None else env
    homes = [("XDG_CONFIG_HOME", ())]
    if platform == "win32":
        homes += [("APPDATA", ()), ("USERPROFILE", ("AppData", "Roaming"))]
    homes += [("HOME", (".config",))]
    for var, suffix in homes:
        value = env.get(var)
        if value:
            return pathlib.Path(value, *suffix, "cortexkit", "broca")
    raise ConfigHomeError("cannot resolve broca's config home: no config-home variable is set")


def default_state_root(env: Optional[Mapping[str, str]] = None, *, platform: str = sys.platform) -> pathlib.Path:
    """The state root the daemon would use under `env` (the process environment by default).

    Empty values count as unset, as they do for the daemon. Raises StateRootError when nothing
    resolves, or when the data home that wins is a relative path.
    """
    env = os.environ if env is None else env
    explicit = env.get(STATE_ROOT_ENV)
    if explicit:
        return pathlib.Path(explicit)
    homes = [("XDG_DATA_HOME", ())]
    if platform == "win32":
        homes += [("APPDATA", ()), ("USERPROFILE", ("AppData", "Roaming"))]
    homes += [("HOME", (".local", "share"))]
    for var, suffix in homes:
        value = env.get(var)
        if not value:
            continue
        data_home = pathlib.Path(value, *suffix)
        if not data_home.is_absolute():
            raise StateRootError(
                f"{var} is the relative path {value!r}; set it to an absolute path, or set "
                f"{STATE_ROOT_ENV}, or pass --state-root"
            )
        return data_home / "cortexkit" / "broca"
    variables = f"{STATE_ROOT_ENV}, XDG_DATA_HOME and HOME"
    if platform == "win32":
        variables = f"{STATE_ROOT_ENV}, XDG_DATA_HOME, APPDATA, USERPROFILE and HOME"
    raise StateRootError(
        f"cannot resolve broca's state root: {variables} are all "
        "unset; set one of them or pass --state-root"
    )
