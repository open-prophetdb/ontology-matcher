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
from ontology_matcher.metabolite.custom_types import MetaboliteOntologyFileFormat

logger = logging.getLogger("ontology_matcher.metabolite")

METABOLITE_DICT = OntologyType(
    type="Metabolite",
    default="HMDB",
    choices=["HMDB", "DrugBank", "PUBCHEM", "CHEBI", "MESH", "UMLS", "CHEMBL"],
)


class MetaboliteOntologyConverter(OntologyBaseConverter):
    """Convert the metabolite id to a standard format for the knowledge graph."""

    def __init__(
        self, ids, strategy=Strategy.MIXTURE, batch_size: int = 300, sleep_time: int = 3
    ):
        """Initialize the Metabolite class for id conversion.

        Args:
            ids (List[str]): A list of metabolite ids (Currently support DrugBank, PUBCHEM, CHEBI, MESH, UMLS, CHEMBL etc.).
            strategy (Strategy, optional): The strategy to keep the results. Defaults to Strategy.MIXTURE, it means that the results will mix different database ids.
            batch_size (int, optional): The batch size for each request. Defaults to 300.
            sleep_time (int, optional): The sleep time between each request. Defaults to 3.
        """
        super(MetaboliteOntologyConverter, self).__init__(
            ontology_type=METABOLITE_DICT,
            ids=ids,
            strategy=strategy,
            batch_size=batch_size,
            sleep_time=sleep_time,
        )

        # More details on the database_url can be found here: https://docs.mychem.info/en/latest/
        self._database_url = "https://mychem.info"
        logger.info(
            "The formatter will use the MyChem API to convert the metabolite ids."
        )

    @property
    def ontology_links(self) -> Dict[str, str]:
        return {
            "HMDB": "https://hmdb.ca/metabolites/",
            "DrugBank": "https://go.drugbank.com/drugs",
            "PUBCHEM": "https://pubchem.ncbi.nlm.nih.gov/",
            "CHEBI": "https://www.ebi.ac.uk/chebi/init.do",
            "MESH": "https://meshb.nlm.nih.gov/search",
            "UMLS": "https://www.nlm.nih.gov/research/umls/",
            "CHEMBL": "https://www.ebi.ac.uk/chembl/",
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

            logger.info("Processing %s to %s" % (i, i + self._batch_size))

            grouped_ids = make_grouped_ids(batch_ids)

            groups = grouped_ids.id_dict.keys()

            for group in groups:
                ids = grouped_ids.id_dict.get(group)
                if ids:
                    request = MyChemical(
                        list(map(lambda x: f"{group}:{x}", ids)), EntityType.METABOLITE
                    )
                    results = request.parse()

                    for result in results:
                        default_id = result.get(METABOLITE_DICT.default)
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
                            self.add_converted_id_dict(result)

        return ConversionResult(
            ids=self._ids,
            strategy=self._strategy,
            converted_ids=self._converted_ids,
            databases=self._databases,
            default_database=self._default_database,
            database_url=self._database_url,
            failed_ids=self._failed_ids,
        )


class MetaboliteOntologyFormatter(BaseOntologyFormatter):
    """Format the disease ontology file."""

    def __init__(
        self,
        filepath: Union[str, Path],
        conversion_result: Optional[ConversionResult] = None,
        **kwargs,
    ) -> None:
        """Initialize the MetaboliteOntologyFormatter class.

        Args:
            filepath (Union[str, Path]): The path of the disease ontology file. Only support csv and tsv file.
            conversion_result (ConversionResult, optional): The results of id conversion. Defaults to None.
            **kwargs: The keyword arguments for the Metabolite class.
        """
        super().__init__(
            filepath,
            file_format_cls=MetaboliteOntologyFileFormat,
            ontology_converter=MetaboliteOntologyConverter,
            conversion_result=conversion_result,
            ontology_type=METABOLITE_DICT,
            **kwargs,
        )

    def format_by_metadata(
        self, new_row: Dict[str, Any], metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        new_row = self.default_format_by_metadata(new_row, metadata)

        return new_row

    def format(self):
        """Format the metabolite ontology file.

        Returns:
            self: The MetaboliteOntologyFormatter instance.
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
    metabolite = MetaboliteOntologyConverter(ids)
    result = metabolite.convert()
    print(result)
