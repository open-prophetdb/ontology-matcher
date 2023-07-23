from pathlib import Path
from ..ontology_formatter import BaseOntologyFileFormat


class DiseaseOntologyFileFormat(BaseOntologyFileFormat):
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
                    "DOID:4001\tovarian carcinoma\tDisease\tDOID\n",
                    "MESH:D015673\tFatigue Syndrom, Chronic\tDisease\tDOID\n",
                ]
            )
