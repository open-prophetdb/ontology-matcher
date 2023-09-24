import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Union, Optional, Any
from ontology_matcher.apis import MyChemical, EntityType
from ontology_matcher.ontology_formatter import (
    OntologyType,
    Strategy,
    ConversionResult,
    FailedId,
    OntologyBaseConverter,
    BaseOntologyFormatter,
    make_grouped_ids,
)
from ontology_matcher.compound.custom_types import CompoundOntologyFileFormat

logger = logging.getLogger("ontology_matcher.compound")

COMPOUND_DICT = OntologyType(
    type="Compound",
    default="DrugBank",
    choices=["DrugBank", "PUBCHEM", "CHEBI", "MESH", "UMLS", "CHEMBL", "HMDB"],
)


class CompoundOntologyConverter(OntologyBaseConverter):
    """Convert the compound id to a standard format for the knowledge graph."""

    def __init__(
        self, ids, strategy=Strategy.MIXTURE, batch_size: int = 300, sleep_time: int = 3
    ):
        """Initialize the Compound class for id conversion.

        Args:
            ids (List[str]): A list of compound ids (Currently support DrugBank, PUBCHEM, CHEBI, MESH, UMLS, CHEMBL etc.).
            strategy (Strategy, optional): The strategy to keep the results. Defaults to Strategy.MIXTURE, it means that the results will mix different database ids.
            batch_size (int, optional): The batch size for each request. Defaults to 300.
            sleep_time (int, optional): The sleep time between each request. Defaults to 3.
        """
        super(CompoundOntologyConverter, self).__init__(
            ontology_type=COMPOUND_DICT,
            ids=ids,
            strategy=strategy,
            batch_size=batch_size,
            sleep_time=sleep_time,
        )

        # More details on the database_url can be found here: https://docs.mychem.info/en/latest/
        self._database_url = "https://mychem.info"
        logger.info(
            "The formatter will use the MyChem API to convert the compound ids."
        )

    @property
    def ontology_links(self) -> Dict[str, str]:
        return {
            "DrugBank": "https://go.drugbank.com/drugs",
            "PUBCHEM": "https://pubchem.ncbi.nlm.nih.gov/",
            "CHEBI": "https://www.ebi.ac.uk/chebi/init.do",
            "MESH": "https://meshb.nlm.nih.gov/search",
            "UMLS": "https://www.nlm.nih.gov/research/umls/",
            "CHEMBL": "https://www.ebi.ac.uk/chembl/",
            "HMDB": "https://hmdb.ca/"
        }
    
    def check_batch_size(self):
        """Check the batch size."""
        self.default_check_batch_size()

    def convert(self) -> ConversionResult:
        """Convert the ids to different databases.

        Returns:
            ConversionResult: The results of id conversion.
        """
        # Cannot use the parallel processing, otherwise the index order will not be correct.
        self._ids = sorted(self._ids)
        for i in range(0, len(self._ids), self._batch_size):
            batch_ids = self._ids[i : i + self._batch_size]

            logger.info("Processing %s to %s" % (i + self._batch_size, i + self._batch_size))

            grouped_ids = make_grouped_ids(batch_ids)

            groups = grouped_ids.id_dict.keys()

            for group in groups:
                ids = grouped_ids.id_dict.get(group)
                if ids:
                    request = MyChemical(
                        list(map(lambda x: f"{group}:{x}", ids)), EntityType.COMPOUND
                    )
                    results = request.parse()

                    for result in results:
                        default_id = result.get(COMPOUND_DICT.default)
                        if isinstance(default_id, list) and len(default_id) > 1:
                            self.add_failed_id(
                                FailedId(
                                    id=result.get("raw_id", ""),
                                    idx=result.get("idx", 0),
                                    reason="Multiple results found",
                                )
                            )
                        else:
                            logger.debug("result: %s", result)
                            self.add_converted_id(result)

        return ConversionResult(
            ids=self._ids,
            strategy=self._strategy,
            converted_ids=self._converted_ids,
            databases=self._databases,
            default_database=self._default_database,
            database_url=self._database_url,
            failed_ids=self._failed_ids,
        )


class CompoundOntologyFormatter(BaseOntologyFormatter):
    """Format the disease ontology file."""

    def __init__(
        self,
        filepath: Union[str, Path],
        conversion_result: Optional[ConversionResult] = None,
        **kwargs,
    ) -> None:
        """Initialize the CompoundOntologyFormatter class.

        Args:
            filepath (Union[str, Path]): The path of the disease ontology file. Only support csv and tsv file.
            conversion_result (ConversionResult, optional): The results of id conversion. Defaults to None.
            **kwargs: The keyword arguments for the Compound class.
        """
        super().__init__(
            filepath,
            file_format_cls=CompoundOntologyFileFormat,
            ontology_converter=CompoundOntologyConverter,
            conversion_result=conversion_result,
            ontology_type=COMPOUND_DICT,
            **kwargs,
        )

    def format_by_metadata(
        self, new_row: Dict[str, Any], metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        new_row = self.default_format_by_metadata(new_row, metadata)

        return new_row

    def format(self):
        """Format the compound ontology file.

        Returns:
            self: The CompoundOntologyFormatter instance.
        """
        self.default_format()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    ids = [
        "DrugBank:DB01628",
        "MESH:D004249",
        "PUBCHEM:123619",
        "CHEBI:6339",
        "UMLS:C0972314",
    ]
    compound = CompoundOntologyConverter(ids)
    result = compound.convert()
    print(result)
