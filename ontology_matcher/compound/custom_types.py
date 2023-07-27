from pathlib import Path
from ontology_matcher.ontology_formatter import BaseOntologyFileFormat


class CompoundOntologyFileFormat(BaseOntologyFileFormat):
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
                    "DrugBank:DB01628\tETORICOXIB\tCompound\tDrugBank\n",
                    "DrugBank:DB01627\tLincomycin\tCompound\tDrugBank\n",
                ]
            )
