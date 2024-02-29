from typing import Optional

import asyncclick as click
from airbyte_protocol.models import ConfiguredAirbyteCatalog

from live_tests.regression_tests.run_tests import Command, run
from live_tests.utils.common import get_connector_config, get_state


@click.command()
@click.option(
    "--connector-name",
    help=(
        "\"Technical\" name of the connector to be tested (e.g. `source-faker`)."
    ),
    default="latest",
    type=str,
)
@click.option(
    "--control-image-name",
    help=(
        "Control version of the connector to be tested. Records will be downloaded from this container and used as expected records for the target version."
    ),
    default="latest",
    type=str,
)
@click.option(
    "--target-image-name",
    help=("Target version of the connector being tested."),
    default="dev",
    type=str,
)
@click.option(
    "--output-directory",
    help=("Directory in which connector output and test results should be stored."),
    default="/tmp/test_output",
    type=str,
)
@click.option(
    "--command",
    help=("Airbyte command."),
    default="all",
    type=click.Choice([c.value for c in Command] + ["all"]),
    required=True
)
@click.option(
    "--config-path",
    help=("Path to the connector config."),
    default=None,
    type=str,
)
@click.option(
    "--catalog-path",
    help=("Path to the connector catalog."),
    default=None,
    type=str,
)
@click.option(
    "--state-path",
    help=("Path to the connector state."),
    default=None,
    type=str,
)
async def main(
    connector_name: str,
    control_image_name: str,
    target_image_name: str,
    output_directory: str,
    command: Optional[str],
    config_path: Optional[str],
    catalog_path: Optional[str],
    state_path: Optional[str],
):
    if control_image_name in ("dev", "latest"):
        control_image_name = f"airbyte/{connector_name}:{control_image_name}"

    if target_image_name in ("dev", "latest"):
        target_image_name = f"airbyte/{connector_name}:{target_image_name}"

    # TODO: add backend options
    if not output_directory:
        raise NotImplementedError(f"An output directory is required; only file backends are currently supported.")

    config = get_connector_config(config_path)
    catalog = ConfiguredAirbyteCatalog.parse_file(catalog_path) if catalog_path else None
    state = get_state(state_path) if state_path else None

    await run(connector_name, control_image_name, target_image_name, output_directory, command, config, catalog, state)
