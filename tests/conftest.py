from __future__ import annotations

import os
import sys


def pytest_configure() -> None:
    """Ensure `import src...` works in all environments.

    In some runners (e.g., CI), invoking `pytest` as an installed script can result
    in the repository root not being present on `sys.path`.
    """

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
