import json
import logging
import os
from pathlib import Path

from flask import Blueprint

from discograph.config import Configuration

log = logging.getLogger(__name__)
"""The logger for the assets module."""


def create_assets_blueprint(config: Configuration) -> Blueprint:
    # Get environment variables.
    # FLASK_DEBUG = os.getenv("FLASK_DEBUG", "0")
    vite_origin = os.getenv("VITE_ORIGIN", "http://localhost:5173")

    # Set application constants.
    is_gunicorn = "gunicorn" in os.environ.get("SERVER_SOFTWARE", "")
    is_production = config.get("PRODUCTION", False)

    log.info(f"is_gunicorn: {is_gunicorn}")
    log.info(f"is_production: {is_production}")

    project_path = Path(os.path.dirname(os.path.abspath(__file__)))

    # Create assets blueprint that stores all Vite-related functionality.
    assets_blueprint = Blueprint(
        "assets",
        __name__,
        static_folder="../frontend/source_compiled/bundled",
        static_url_path="/assets/bundled",
    )

    # Load manifest file in the production environment.
    manifest = {}
    if is_production:
        manifest_path = project_path / "../frontend/source_compiled/manifest.json"
        try:
            with open(manifest_path, "r") as content:
                manifest = json.load(content)
        except OSError as exception:
            raise OSError(
                f"Manifest file not found at {manifest_path}. Run `npm run build`."
            ) from exception

    # Add `asset()` function and `is_production` to app context.
    @assets_blueprint.app_context_processor
    def add_context():
        def dev_asset(file_path):
            log.debug(f"dev asset: {file_path}")
            return f"{vite_origin}/assets/{file_path}"

        def prod_asset(file_path):
            log.debug(f"prod asset: {file_path}")
            try:
                return f"/assets/{manifest[file_path]['file']}"
            except:
                log.error(f"Asset not found: {file_path}")
                return "asset-not-found"

        return {
            "asset": prod_asset if is_production else dev_asset,
            "is_production": is_production,
        }

    return assets_blueprint
