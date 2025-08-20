# src/brazil_race_classifier/__init__.py
from importlib.metadata import PackageNotFoundError, version

try:
    # Use project name as declared in pyproject.toml
    __version__ = version("brazil-race-classifier")
except PackageNotFoundError:
    __version__ = "0.0.0+dev"

# Default configuration files for CLI
TSE_CONFIG     = "configs/tse_urls.yaml"
PROJECT_CONFIG = "configs/project.yaml"

__all__ = ["__version__", "TSE_CONFIG", "PROJECT_CONFIG"]
