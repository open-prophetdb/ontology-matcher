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

    def _format_by_metadata(
        self, new_row: Dict[str, Any], metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        new_row[self.file_format_cls.NAME] = metadata.get("name") or new_row.get("name")
        new_row[self.file_format_cls.DESCRIPTION] = metadata.get(
            "description"
        ) or new_row.get("description")

        synonyms = metadata.get("synonyms") or new_row.get("synonyms")

        if synonyms and type(synonyms) == list:
            synonyms = map(lambda x: str(x), synonyms)
            new_row[self.file_format_cls.SYNONYMS] = self.join_lst(synonyms)
        else:
            new_row[self.file_format_cls.SYNONYMS] = synonyms

        pmids = metadata.get("pmids") or new_row.get("pmids")

        if pmids and type(pmids) == list:
            new_row[self.file_format_cls.PMIDS] = self.join_lst(pmids)
        else:
            new_row[self.file_format_cls.PMIDS] = pmids

        new_row[self.file_format_cls.XREFS] = metadata.get("xrefs") or new_row.get(
            "xrefs"
        )
        return new_row

    def format(self):
        """Format the metabolite ontology file.

        Returns:
            self: The MetaboliteOntologyFormatter instance.
        """
        formated_data = []
        failed_formatted_data = []

        for converted_id in self.conversion_result.converted_ids:
            logger.debug("All keys: %s" % converted_id.__dict__)
            raw_id = converted_id.get("raw_id")
            id = converted_id.get(self.ontology_type.default)
            record = self.get_raw_record(raw_id)
            columns = self._expected_columns + self._optional_columns
            new_row = {key: self.format_record_value(record, key) for key in columns}

            metadata = converted_id.get_metadata()

            if metadata:
                new_row = self._format_by_metadata(new_row, metadata)

            if id is None or len(id) == 0:
                # Keep the original record if the id does not match the default prefix.
                unique_ids = self.get_alias_ids(converted_id)
                new_row[self.file_format_cls.XREFS] = self.join_lst(unique_ids)
                formated_data.append(new_row)
                logger.debug("No results found for %s, %s" % (raw_id, new_row))
            elif type(id) == list and len(id) > 1:
                new_row[self.file_format_cls.XREFS] = self.join_lst(id)
                new_row["reason"] = "Multiple results found"
                failed_formatted_data.append(new_row)
            else:
                if type(id) == list and len(id) == 1:
                    id = id[0]

                new_row["raw_id"] = raw_id
                new_row[self.file_format_cls.ID] = str(id)
                new_row[self.file_format_cls.RESOURCE] = self.ontology_type.default
                new_row[self.file_format_cls.LABEL] = self.ontology_type.type

                unique_ids = self.get_alias_ids(converted_id)
                new_row[self.file_format_cls.XREFS] = self.join_lst(unique_ids)

                formated_data.append(new_row)

        for failed_id in self.conversion_result.failed_ids:
            id = failed_id.id
            prefix, value = id.split(":")
            record = self.get_raw_record(id)
            columns = self._expected_columns + self._optional_columns
            new_row = {key: self.format_record_value(record, key) for key in columns}
            new_row[self.file_format_cls.ID] = id
            new_row[self.file_format_cls.LABEL] = self.ontology_type.type
            new_row[self.file_format_cls.RESOURCE] = prefix

            # Keep the original record if the id match the default prefix.
            # If we allow the mixture strategy, we will keep the original record even if the id does not match the default prefix. So we don't have the failed data to return.
            if (
                prefix == self.ontology_type.default
                or self.conversion_result.strategy == Strategy.MIXTURE
            ):
                formated_data.append(new_row)
            else:
                new_row["reason"] = failed_id.reason
                failed_formatted_data.append(new_row)

        if len(formated_data) > 0:
            self._formatted_data = pd.DataFrame(formated_data)

        if len(failed_formatted_data) > 0:
            self._failed_formatted_data = pd.DataFrame(failed_formatted_data)

        return self


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
