import os
import json
import click
import logging
import coloredlogs
import verboselogs
from typing import Type, Union
from ontology_matcher import (
    ONTOLOGY_DICT,
    BaseOntologyFormatter,
    ONTOLOGY_DICT_KEYS,
    ONTOLOGY_TYPE_DICT,
    ONTOLOGY_FILE_FORMAT_DICT,
)
from ontology_matcher.ontology_formatter import CustomJSONDecoder

verboselogs.install()
# Use the logger name instead of the module name
coloredlogs.install(
    fmt="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s"
)
logger = logging.getLogger("cli")

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
@click.option("--batch-size", "-b", help="Batch size, default is 300.", default=300)
@click.option("--sleep-time", "-s", help="Sleep time, default is 3.", default=3)
@click.option("--debug", "-d", help="Debug mode", is_flag=True)
@click.option(
    "--reformat",
    "-r",
    help="Rerun the formatter, but not fetching the data again.",
    is_flag=True,
)
def ontology(
    input_file,
    output_file,
    ontology_type,
    batch_size,
    sleep_time,
    debug=False,
    reformat=False,
):
    """Ontology matcher"""
    if debug:
        logger.setLevel(logging.DEBUG)

    ontology_formatter_cls: Union[
        Type[BaseOntologyFormatter], None
    ] = ONTOLOGY_DICT.get(ontology_type)
    if ontology_formatter_cls is None:
        raise ValueError("Ontology type not supported currently.")

    conversion_result = None
    if reformat:
        json_file = output_file.replace(".tsv", ".json")
        if not os.path.isfile(json_file):
            raise ValueError("Cannot find the json file, please rerun the command without --reformat flag.")
        else:
            saved_data = json.load(open(json_file, "r"), cls=CustomJSONDecoder)
            conversion_result = saved_data.get("conversion_result")

            if conversion_result is None:
                logger.warning("Cannot find the conversion result in the json file, so we will fetch the data again.")

    elif os.path.exists(output_file):
        raise ValueError("The output file already exists, if you want to reformat, please add --reformat flag or delete the output file and the json file, then rerun the command.")

    ontology_formatter = ontology_formatter_cls(
        filepath=input_file,
        batch_size=batch_size,
        sleep_time=sleep_time,
        conversion_result=conversion_result,
    )
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
