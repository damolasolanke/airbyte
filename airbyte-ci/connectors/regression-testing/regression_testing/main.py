import asyncio
import logging
import sys
from typing import Dict, Optional

import click
import dagger
from airbyte_protocol.models import ConfiguredAirbyteCatalog

from .backends import BaseBackend, FileBackend
from .connector_runner import ConnectorRunner, SecretDict
from .utils import get_connector, get_connector_config, get_state

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


async def _main(
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

    async with dagger.Connection(config=dagger.Config(log_output=sys.stderr)) as client:
        control_container = await get_connector(client, control_image_name)
        control_version = control_image_name.split(":")[-1]

        target_container = await get_connector(client, target_image_name)
        target_version = target_image_name.split(":")[-1]

        # TODO: maybe use proxy to cache the response from the first round and use the cache for the second round
        #   (this may only make sense for syncs with an input state)
        tasks = [
            dispatch(
                container,
                FileBackend(f"{output_directory}/{version}"),
                f"{output_directory}/{version}",
                command,
                config,
                catalog,
                state,
            ) for version, container in [(control_version, control_container), (target_version, target_container)]
        ]
        await asyncio.gather(*tasks)


async def dispatch(
    container: dagger.Container,
    backend: BaseBackend,
    output_directory: str,
    command: str,
    config: Optional[SecretDict],
    catalog: Optional[ConfiguredAirbyteCatalog],
    state: Optional[Dict],
):
    if command == "read":
        runner = ConnectorRunner(container, backend, output_directory)
        if state:
            await runner.call_read_with_state(config, catalog, state)
        else:
            await runner.call_read(config, catalog)

    else:
        raise NotImplementedError(f"{command} is not yet implemented")


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
    default="read",
    type=click.Choice(["spec", "check", "discover", "read"]),
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
def main(
    connector_name: str,
    control_image_name: str,
    target_image_name: str,
    output_directory: str,
    command: Optional[str],
    config_path: Optional[str],
    catalog_path: Optional[str],
    state_path: Optional[str],
):
    asyncio.run(
        _main(
            connector_name,
            control_image_name,
            target_image_name,
            output_directory,
            command,
            config_path,
            catalog_path,
            state_path,
        )
    )
