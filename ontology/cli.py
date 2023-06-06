import click
from typing import Type, Union
from ontology import ontology_dict, BaseOntologyFormatter


@click.command()
@click.option(
    "--input-file",
    "-i",
    help="Path to input file",
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
    type=click.Choice(
        [
            "disease",
            "gene",
            "compound",
            "anatomy",
            "pathway",
            "cellular_component",
            "molecular_function",
            "biological_process",
            "pharmacologic_class",
            "side_effect",
            "symptom",
            "protein",
            "metabolite",
        ]
    ),
)
@click.option("--batch-size", "-b", help="Batch size, default is 300.", default=300)
@click.option("--sleep-time", "-s", help="Sleep time, default is 3.", default=3)
def cli(input_file, output_file, ontology_type, batch_size, sleep_time):
    """Ontology matcher"""
    ontology_formatter_cls: Union[
        Type[BaseOntologyFormatter], None
    ] = ontology_dict.get(ontology_type)
    if ontology_formatter_cls is None:
        raise ValueError("Ontology type not supported currently.")

    ontology_formatter = ontology_formatter_cls(
        filepath=input_file, batch_size=batch_size, sleep_time=sleep_time
    )
    ontology_formatter.format()
    ontology_formatter.write(output_file)
