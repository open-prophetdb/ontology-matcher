import re
import time
import requests
import pandas as pd
from pathlib import Path
from typing import Union, List, Optional, Dict
from tenacity import retry, stop_after_attempt, wait_random
from ontology_matcher.ontology_formatter import (
    OntologyType,
    Strategy,
    ConversionResult,
    FailedId,
    OntologyBaseConverter,
    BaseOntologyFormatter,
    NoResultException,
)
from ontology_matcher.symptom.custom_types import SymptomOntologyFileFormat

# SYMP: Symptom Ontology ID, https://raw.githubusercontent.com/SymptomOntology/SymptomOntology/v2022-11-30/src/ontology/symp.owl; https://bioportal.bioontology.org/ontologies/SYMP
# UMLS: Unified Medical Language System, https://www.nlm.nih.gov/research/umls/
# MESH: Medical Subject Headings, https://www.nlm.nih.gov/mesh/
# HP: Human Phenotype Ontology, https://hpo.jax.org/app/

SYMPTOM_DICT = OntologyType(
    type="Symptom", default="MESH", choices=["SYMP", "MESH", "UMLS", "HP"]
)


class SymptomOntologyConverter(OntologyBaseConverter):
    """Convert the symptom id to a standard format for the knowledge graph."""

    def __init__(
        self, ids, strategy=Strategy.MIXTURE, batch_size: int = 300, sleep_time: int = 3
    ):
        """Initialize the Symptom class for id conversion.

        Args:
            ids (List[str]): A list of symptom ids (Currently support SYMP, UMLS and MESH).
            strategy (Strategy, optional): The strategy to keep the results. Defaults to Strategy.MIXTURE, it means that the results will mix different database ids.
            batch_size (int, optional): The batch size for each request. Defaults to 300.
            sleep_time (int, optional): The sleep time between each request. Defaults to 3.
        """
        super().__init__(
            ontology_type=SYMPTOM_DICT,
            ids=ids,
            strategy=strategy,
            batch_size=batch_size,
            sleep_time=sleep_time,
        )

        # More details on the database_url can be found here: https://www.ebi.ac.uk/spot/oxo/index
        self._database_url = "https://www.ebi.ac.uk/spot/oxo/api/search"
        print("The formatter will use the OXO API to convert the symptom ids.")

    @property
    def ontology_links(self) -> Dict[str, str]:
        return {
            "UMLS": "https://www.nlm.nih.gov/research/umls/",
            "MESH": "https://www.nlm.nih.gov/mesh/",
            "SYMP": "https://bioportal.bioontology.org/ontologies/SYMP",
            "HP": "https://hpo.jax.org/app/",
        }
    
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

        print(
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
                    self._converted_ids.append(converted_id_dict)

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
        payload = {
            "ids": ids,
            "inputSource": None,
            "mappingTarget": self.databases,
            "mappingSource": self.databases,
            "distance": 1,
        }

        results = requests.post(
            self._database_url,
            headers=headers,
            json=payload,
            params={"size": self._batch_size},
        )

        print("Requests: %s\n%s" % (results.json(), payload))
        return results.json()

    def convert(self) -> ConversionResult:
        """Convert the ids to different databases.

        Returns:
            ConversionResult: The results of id conversion.
        """
        # Cannot use the parallel processing, otherwise the index order will not be correct.
        for i in range(0, len(self._ids), self._batch_size):
            batch_ids = self._ids[i : i + self._batch_size]
            self._fetch_format_data(batch_ids)
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


class SymptomOntologyFormatter(BaseOntologyFormatter):
    """Format the symptom ontology file."""

    def __init__(
        self,
        filepath: Union[str, Path],
        dict: Optional[ConversionResult] = None,
        **kwargs,
    ) -> None:
        """Initialize the SymptomOntologyFormatter class.

        Args:
            filepath (Union[str, Path]): The path of the symptom ontology file. Only support csv and tsv file.
            dict (ConversionResult, optional): The results of id conversion. Defaults to None.
            **kwargs: The keyword arguments for the Symptom class.
        """
        super().__init__(
            filepath,
            file_format_cls=SymptomOntologyFileFormat,
            ontology_converter=SymptomOntologyConverter,
            dict=dict,
            ontology_type=SYMPTOM_DICT,
            **kwargs,
        )

    def format(self):
        """Format the symptom ontology file.

        Returns:
            self: The SymptomOntologyFormatter instance.
        """
        formated_data = []
        failed_formatted_data = []

        for converted_id in self._dict.converted_ids:
            raw_id = converted_id.get("raw_id")
            id = converted_id.get(SYMPTOM_DICT.default)
            record = self.get_raw_record(raw_id)
            columns = self._expected_columns + self._optional_columns
            new_row = {key: self.format_record_value(record, key) for key in columns}

            if id is None:
                # Keep the original record if the id does not match the default prefix.
                unique_ids = self.get_alias_ids(converted_id)
                new_row[self.file_format_cls.XREFS] = "|".join(unique_ids)
                formated_data.append(new_row)
            elif type(id) == list and len(id) > 1:
                new_row[self.file_format_cls.XREFS] = "|".join(id)
                new_row["reason"] = "Multiple results found"
                failed_formatted_data.append(new_row)
            else:
                if type(id) == list and len(id) == 1:
                    id = id[0]

                new_row[self.file_format_cls.ID] = str(id)
                new_row[self.file_format_cls.RESOURCE] = self.ontology_type.default
                new_row[self.file_format_cls.LABEL] = self.ontology_type.type

                unique_ids = self.get_alias_ids(converted_id)
                new_row[self.file_format_cls.XREFS] = "|".join(unique_ids)

                formated_data.append(new_row)

        for failed_id in self._dict.failed_ids:
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
            if (
                prefix == self.ontology_type.default
                or self._dict.strategy == Strategy.MIXTURE
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
        "SYMP:0000099",
        "SYMP:0000259",
        "SYMP:0000729",
        "MESH:D010146",
        "MESH:D000270",
        "MESH:D000326",
        "MESH:D000334",
    ]
    symptom = SymptomOntologyConverter(ids)
    result = symptom.convert()
    print(result)
