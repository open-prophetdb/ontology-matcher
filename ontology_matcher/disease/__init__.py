import re
import time
import logging
import requests
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_random
from pathlib import Path
from typing import Dict, Union, List, Optional, Any
from ontology_matcher.apis import MyDisease
from ontology_matcher.ontology_formatter import (
    OntologyType,
    Strategy,
    ConversionResult,
    FailedId,
    OntologyBaseConverter,
    BaseOntologyFormatter,
    NoResultException,
)
from ontology_matcher.disease.custom_types import DiseaseOntologyFileFormat

logger = logging.getLogger("ontology_matcher.disease")

DISEASE_DICT = OntologyType(
    type="Disease",
    default="MONDO",
    choices=["MONDO", "DOID", "MESH", "OMIM", "ICD-9", "HP", "ICD10CM", "ORDO", "UMLS"],
)


class DiseaseOntologyConverter(OntologyBaseConverter):
    """Convert the disease id to a standard format for the knowledge graph."""

    def __init__(
        self, ids, strategy=Strategy.MIXTURE, batch_size: int = 300, sleep_time: int = 3
    ):
        """Initialize the Disease class for id conversion.

        Args:
            ids (List[str]): A list of disease ids (Currently support MONDO, DOID and MESH).
            strategy (Strategy, optional): The strategy to keep the results. Defaults to Strategy.MIXTURE, it means that the results will mix different database ids.
            batch_size (int, optional): The batch size for each request. Defaults to 300.
            sleep_time (int, optional): The sleep time between each request. Defaults to 3.
        """
        super(DiseaseOntologyConverter, self).__init__(
            ontology_type=DISEASE_DICT,
            ids=ids,
            strategy=strategy,
            batch_size=batch_size,
            sleep_time=sleep_time,
        )

        # More details on the database_url can be found here: https://www.ebi.ac.uk/spot/oxo/index
        self._database_url = "https://www.ebi.ac.uk/spot/oxo/api/search"
        logger.info("The formatter will use the OXO API to convert the disease ids.")

    @property
    def ontology_links(self) -> Dict[str, str]:
        return {
            "MONDO": "https://www.ebi.ac.uk/ols4/ontologies/mondo",
            "DOID": "https://www.ebi.ac.uk/ols4/ontologies/doid",
            "MESH": "https://meshb.nlm.nih.gov/search",
            "OMIM": "https://www.omim.org/",
            "ICD-9": "https://www.cdc.gov/nchs/icd/icd9.htm",
            "HP": "https://hpo.jax.org/app/",
            "ICD10CM": "https://www.cdc.gov/nchs/icd/icd-10-cm.htm",
            # OxO Cannot support SNOMED currently. Please access https://www.ebi.ac.uk/spot/oxo/api/datasources/SNOMED to check.
            # "SNOMED": "https://www.snomed.org/",
            "ORDO": "https://www.orpha.net/consor/cgi-bin/index.php",
            "UMLS": "https://www.nlm.nih.gov/research/umls/",
        }

    def _format_response(self, response: dict, batch_ids: List[str]) -> None:
        """Format the response from the OXO API.

        Args:
            response (dict): The response from the OXO API. It was generated by the resp.json() method.
            batch_ids (List[str]): The list of ids for the current batch.

        Raises:
            Exception: If no results found.

        Returns:
            None
        """
        search_results = response.get("_embedded", {}).get("searchResults", [])
        if len(search_results) == 0:
            raise NoResultException()

        logger.info(
            "Batch size: %s, results size: %s" % (len(batch_ids), len(search_results))
        )
        for index, id in enumerate(batch_ids):
            prefix, value = id.split(":")
            if prefix not in self.databases:
                failed_id = FailedId(
                    idx=index,
                    id=id,
                    reason="Invalid prefix, only support %s" % self.databases,
                )
                self._failed_ids.append(failed_id)
                continue

            result = search_results[index]
            mapping_response_list = result.get("mappingResponseList", [])
            if len(mapping_response_list) == 0:
                failed_id = FailedId(idx=index, id=id, reason="No results found")
                self._failed_ids.append(failed_id)
                continue
            else:
                converted_id_dict = {}
                converted_id_dict[prefix] = id
                converted_id_dict["raw_id"] = id
                # OxO don't provide any metadata for the disease ontology. So we will update the metadata later by using the OLS API.
                converted_id_dict["metadata"] = None
                difference = [x for x in self.databases if x != prefix]
                for choice in difference:
                    # The prefix maybe case insensitive, such as MeSH:D015161. But we need to keep all the prefix in upper case.
                    matched = list(
                        filter(
                            lambda x: re.match(
                                r"^%s.*" % choice, x.get("curie"), re.I.IGNORECASE
                            ),
                            mapping_response_list,
                        )
                    )
                    if len(matched) > 0:
                        converted_ids = [
                            f'{choice}:{x.get("curie").split(":")[1]}' for x in matched
                        ]
                        converted_id_dict[choice] = converted_ids
                        converted_id_dict["idx"] = index

                        if choice == self.default_database and len(converted_ids) > 1:
                            failed_id = FailedId(
                                idx=index, id=id, reason="Multiple results found"
                            )
                            self._failed_ids.append(failed_id)
                            # Abandon the converted_id_dict, otherwise the converted_ids will be added to the converted_ids list.
                            converted_id_dict = {}
                            break

                        if self._strategy == Strategy.UNIQUE and len(converted_ids) > 1:
                            failed_id = FailedId(
                                idx=index,
                                id=id,
                                reason="The strategy is unique, but multiple results found",
                            )
                            self._failed_ids.append(failed_id)
                            # Abandon the converted_id_dict, otherwise the converted_ids will be added to the converted_ids list.
                            converted_id_dict = {}
                            break
                    else:
                        converted_id_dict[choice] = None

                if converted_id_dict:
                    self.add_converted_id(converted_id_dict)

    @retry(stop=stop_after_attempt(5), wait=wait_random(min=1, max=15))
    def _fetch_ids(self, ids) -> dict:
        """Fetch the ids from the OXO API.

        Args:
            ids (List[str]): A list of ids.

        Returns:
            dict: The response from the OXO API which was generated by the resp.json() method.
        """
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        }
        results = requests.post(
            self._database_url,
            headers=headers,
            json={
                "ids": ids,
                "inputSource": None,
                "mappingTarget": self.databases,
                "mappingSource": self.databases,
                "distance": 1,
            },
            params={"size": self._batch_size},
        )

        return results.json()

    @retry(stop=stop_after_attempt(5), wait=wait_random(min=1, max=15))
    def _fetch_format_data(self, ids: List[str]) -> None:
        """Fetch and format the ids.

        Args:
            ids (List[str]): A list of ids.

        Returns:
            None
        """
        response = self._fetch_ids(ids)
        self._format_response(response, ids)

    def convert(self) -> ConversionResult:
        """Convert the ids to different databases.

        Returns:
            ConversionResult: The results of id conversion.
        """
        # Cannot use the parallel processing, otherwise the index order will not be correct.
        for i in range(0, len(self._ids), self._batch_size):
            batch_ids = self._ids[i : i + self._batch_size]
            self._fetch_format_data(batch_ids)
            self._converted_ids = MyDisease.update_metadata(
                self._converted_ids, self.default_database
            )
            time.sleep(self._sleep_time)

        return ConversionResult(
            ids=self._ids,
            strategy=self._strategy,
            converted_ids=self._converted_ids,
            databases=self._databases,
            default_database=self._default_database,
            database_url=self._database_url,
            failed_ids=self._failed_ids,
        )


class DiseaseOntologyFormatter(BaseOntologyFormatter):
    """Format the disease ontology file."""

    def __init__(
        self,
        filepath: Union[str, Path],
        conversion_result: Optional[ConversionResult] = None,
        **kwargs,
    ) -> None:
        """Initialize the DiseaseOntologyFormatter class.

        Args:
            filepath (Union[str, Path]): The path of the disease ontology file. Only support csv and tsv file.
            conversion_result (ConversionResult, optional): The results of id conversion. Defaults to None.
            **kwargs: The keyword arguments for the Disease class.
        """
        super().__init__(
            filepath,
            file_format_cls=DiseaseOntologyFileFormat,
            ontology_converter=DiseaseOntologyConverter,
            conversion_result=conversion_result,
            ontology_type=DISEASE_DICT,
            **kwargs,
        )

    def _format_by_metadata(
        self, new_row: Dict[str, Any], metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        new_row[self.file_format_cls.NAME] = metadata.get("name") or new_row.get("name")
        new_row[self.file_format_cls.DESCRIPTION] = metadata.get(
            "description"
        ) or new_row.get("description")
        new_row[self.file_format_cls.SYNONYMS] = metadata.get(
            "synonyms"
        ) or new_row.get("synonyms")
        return new_row

    def format(self):
        """Format the disease ontology file.

        Returns:
            self: The DiseaseOntologyFormatter instance.
        """
        formated_data = []
        failed_formatted_data = []

        for converted_id in self.conversion_result.converted_ids:
            raw_id = converted_id.get("raw_id")
            id = converted_id.get(self.ontology_type.default)
            record = self.get_raw_record(raw_id)
            columns = self._expected_columns + self._optional_columns
            new_row = {key: self.format_record_value(record, key) for key in columns}

            metadata = converted_id.get_metadata()

            if metadata:
                new_row = self._format_by_metadata(new_row, metadata)

            if id is None:
                # Keep the original record if the id does not match the default prefix.
                unique_ids = self.get_alias_ids(converted_id)
                new_row[self.file_format_cls.XREFS] = "|".join(unique_ids)
                formated_data.append(new_row)
                logger.debug("No results found for %s, %s" % (raw_id, new_row))
            elif type(id) == list and len(id) > 1:
                new_row[self.file_format_cls.XREFS] = "|".join(id)
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
                new_row[self.file_format_cls.XREFS] = "|".join(unique_ids)

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
            new_row[self.file_format_cls.XREFS] = ""

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
    ids = [
        "DOID:7402",
        "MESH:D015673",
        "ICD10CM:C34.9",
        "HP:0030358",
        "ORDO:94063",
        "UMLS:C0007131",
        "ICD-9:349.89",
    ]
    disease = DiseaseOntologyConverter(ids)
    result = disease.convert()
    print(result)
