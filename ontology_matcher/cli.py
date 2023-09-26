import os
import json
import click
import logging
import coloredlogs
import verboselogs
from logging.handlers import RotatingFileHandler
import requests_cache
from typing import Type, Union
from ontology_matcher import (
    ONTOLOGY_DICT,
    BaseOntologyFormatter,
    ONTOLOGY_DICT_KEYS,
    ONTOLOGY_TYPE_DICT,
    ONTOLOGY_FILE_FORMAT_DICT,
)
from ontology_matcher.ontology_formatter import CustomJSONDecoder

logger = logging.getLogger("ontology_matcher.cli")

cli = click.Group()


@cli.command(help="Convert ontology ids.")
@click.option(
    "--input-file",
    "-i",
    help="Path to input file (You can follow the template subcommand to generate a template file.)",
    required=True,
    type=click.Path(file_okay=True, dir_okay=False),
)
@click.option("--output-file", "-o", help="Path to output file", required=True)
@click.option(
    "--ontology-type",
    "-O",
    help="Ontology type",
    type=click.Choice(ONTOLOGY_DICT_KEYS),
)
@click.option(
    "--log-file",
    "-l",
    help="Path to log file",
    default=None,
    type=click.Path(file_okay=True, dir_okay=False),
)
@click.option("--batch-size", "-b", help="Batch size, default is 300.", default=300)
@click.option("--sleep-time", "-s", help="Sleep time, default is 3.", default=3)
@click.option("--debug", "-d", help="Debug mode", is_flag=True, default=False)
@click.option(
    "--reformat",
    "-r",
    help="Rerun the formatter, but not fetching the data again.",
    is_flag=True,
)
@click.option(
    "--disable-cache", "-D", help="Disable the cache", is_flag=True, default=False
)
def ontology(
    input_file,
    output_file,
    ontology_type,
    batch_size,
    sleep_time,
    log_file,
    debug=False,
    reformat=False,
    disable_cache=False,
):
    """Ontology matcher"""
    verboselogs.install()

    if log_file is not None:
        max_log_size = 1024 * 1024 * 100  # 1 MB
        backup_count = 50  # Number of backup log files to keep
        fh = RotatingFileHandler(log_file, maxBytes=max_log_size, backupCount=backup_count)

        # How to set the logging level to DEBUG globally for all imported modules?
        level = logging.DEBUG if debug else logging.INFO
        fh.setLevel(level)
        fh.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s"
            )
        )
        logging.basicConfig(level=level, handlers=[fh])
    else:
        # Use the logger name instead of the module name
        coloredlogs.install(
            level=logging.DEBUG if debug else logging.INFO,
            fmt="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s",
        )

    if not disable_cache:
        logger.info("Enable the cache, you can use --disable-cache to disable it.")
        dbfile = os.path.join(os.path.curdir, "ontology_matcher_cache.sqlite")
        logger.info(
            f"The cache file is {dbfile}, if you encounter any problem, you can delete it or disable cache and rerun the command."
        )

        requests_cache.install_cache(
            cache_name="ontology_matcher_cache",
            backend="sqlite",
            allowable_methods=(
                "GET",
                "POST",
                "PUT",
                "DELETE",
                "HEAD",
                "OPTIONS",
                "TRACE",
            ),
            allowable_codes=(200, 201, 202, 203, 204, 205, 206, 207, 208, 226),
        )
        logging.getLogger("requests_cache").setLevel(logging.DEBUG)
        logger.debug("Enable the logging for requests_cache.")

    conversion_result = None
    json_file = output_file.replace(".tsv", ".json")
    if os.path.isfile(json_file) and not reformat:
        logger.warning(
            "The json file already exists, if you want to reformat, please add --reformat flag or delete the output file and the json file, then rerun the command."
        )
        return

    if reformat:
        if not os.path.isfile(json_file):
            logger.error(
                "Cannot find the json file, please rerun the command without --reformat flag."
            )
            return
        else:
            saved_data = json.load(open(json_file, "r"), cls=CustomJSONDecoder)
            conversion_result = saved_data.get("conversion_result")

    if conversion_result is None:
        logger.warning(
            "Cannot find the conversion result in the json file, so we will fetch the data again."
        )

    ontology_formatter_cls: Union[
        Type[BaseOntologyFormatter], None
    ] = ONTOLOGY_DICT.get(ontology_type)

    if ontology_formatter_cls is None:
        raise ValueError("Ontology type not supported currently.")

    ontology_formatter = ontology_formatter_cls(
        filepath=input_file,
        batch_size=batch_size,
        sleep_time=sleep_time,
        conversion_result=conversion_result,
    )
    ontology_formatter.save_to_json(output_file)
    ontology_formatter.format()
    ontology_formatter.write(output_file)


@cli.command(help="Which ID types are supported.")
@click.option(
    "--ontology-type",
    "-O",
    help="Ontology type",
    required=True,
    type=click.Choice(ONTOLOGY_DICT_KEYS),
)
def idtypes(ontology_type):
    """Generate template for ontology formatter."""
    ot = ONTOLOGY_TYPE_DICT.get(ontology_type)
    if ot is None:
        raise ValueError("Ontology type not supported currently.")
    click.echo("\n".join(ot.choices))


@cli.command(help="Generate input file template")
@click.option(
    "--ontology-type",
    "-O",
    help="Ontology type",
    required=True,
    type=click.Choice(ONTOLOGY_DICT_KEYS),
)
@click.option("--output-file", "-o", help="Path to output file", required=True)
def template(output_file, ontology_type):
    """Generate template for ontology formatter."""
    ot = ONTOLOGY_FILE_FORMAT_DICT.get(ontology_type)
    if ot is None:
        raise ValueError("Ontology type not supported currently.")

    ot.generate_template(output_file)
