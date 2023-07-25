from pathlib import Path
from ontology_matcher.ontology_formatter import BaseOntologyFileFormat


class GeneOntologyFileFormat(BaseOntologyFileFormat):
    @classmethod
    def generate_template(cls, filepath: str | Path):
        """Generate a template file.

        Args:
            filepath (Union[str, Path]): The path of the template file.
        """
        filepath = Path(filepath)
        with open(filepath, "w") as f:
            f.write(f"{cls.ID}\t{cls.NAME}\t{cls.LABEL}\t{cls.RESOURCE}\n")
            f.writelines(
                [
                    "ENTREZ:7157\ttumor protein p53\tGene\tENTREZ\n",
                    "ENTREZ:7100\ttoll like receptor 5\tGene\tENTREZ\n",
                    "HGNC:11998\targinine vasopressin\tGene\tHGNC\n",
                    "ENSEMBL:ENSG00000141510\ttumor protein p53\tGene\tENSEMBL\n",
                    "SYMBOL:TP53\ttumor protein p53\tGene\tSYMBOL\n",
                ]
            )
