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

verboselogs.install()
coloredlogs.install(fmt='%(asctime)s - %(module)s:%(lineno)d - %(levelname)s - %(message)s')
logger = logging.getLogger('cli')

cli = click.Group()


@cli.command(help="Convert ontology ids.")
@click.option(
    "--input-file",
    "-i",
    help="Path to input file (You can follow the template subcommand to generate a template file.)",
    required=True,
    type=click.Path(file_okay=True, dir_okay=False),
)
@click.option(
    "--output-file",
    "-o",
    help="Path to output file",
    required=True,
    type=click.Path(file_okay=False, dir_okay=False),
)
@click.option(
    "--ontology-type",
    "-O",
    help="Ontology type",
    type=click.Choice(ONTOLOGY_DICT_KEYS),
)
@click.option("--batch-size", "-b", help="Batch size, default is 300.", default=300)
@click.option("--sleep-time", "-s", help="Sleep time, default is 3.", default=3)
@click.option("--debug", "-d", help="Debug mode", is_flag=True)
def ontology(input_file, output_file, ontology_type, batch_size, sleep_time, debug=False):
    """Ontology matcher"""
    if debug:
        logger.setLevel(logging.DEBUG)

    ontology_formatter_cls: Union[
        Type[BaseOntologyFormatter], None
    ] = ONTOLOGY_DICT.get(ontology_type)
    if ontology_formatter_cls is None:
        raise ValueError("Ontology type not supported currently.")

    ontology_formatter = ontology_formatter_cls(
        filepath=input_file,
        batch_size=batch_size,
        sleep_time=sleep_time,
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
