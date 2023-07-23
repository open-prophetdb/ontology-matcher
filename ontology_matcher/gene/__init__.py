import time
import mygene
import pandas as pd
from pathlib import Path
from typing import Union, List, Optional, Dict
from ontology_matcher.ontology_formatter import (
    OntologyType,
    Strategy,
    ConversionResult,
    FailedId,
    OntologyBaseConverter,
    BaseOntologyFormatter,
    NoResultException,
)
from ontology_matcher.gene.custom_types import GeneOntologyFileFormat

default_field_dict = {
    "ENTREZ": "entrezgene",
    "ENSEMBL": "ensembl.gene",
    "HGNC": "HGNC",  # It's not consistent with the mygene API. but it's the only way to get the HGNC id.
    "SYMBOL": "symbol",
}

# Get the following fields from the mygene API. Just for getting more information.
additional_fields = [
    "name",
    "taxid",
]

GENE_DICT = OntologyType(
    type="Gene", default="ENTREZ", choices=list(default_field_dict.keys())
)


def get_scope(choice):
    return default_field_dict.get(choice, "unknown")


class GeneOntologyConverter(OntologyBaseConverter):
    """Convert the gene id to a standard format for the knowledge graph."""

    def __init__(
        self, ids, strategy=Strategy.MIXTURE, batch_size: int = 300, sleep_time: int = 3
    ):
        """Initialize the Gene class for id conversion.

        Args:
            ids (List[str]): A list of gene ids (Currently support ENTREZ, ENSEMBL and HGNC).
            strategy (Strategy, optional): The strategy to keep the results. Defaults to Strategy.MIXTURE, it means that the results will mix different database ids.
            batch_size (int, optional): The batch size for each request. Defaults to 300.
            sleep_time (int, optional): The sleep time between each request. Defaults to 3.
        """
        super().__init__(
            ontology_type=GENE_DICT,
            ids=ids,
            strategy=strategy,
            batch_size=batch_size,
            sleep_time=sleep_time,
        )

        self._database_url = "https://mygene.info"
        self._mygene = mygene.MyGeneInfo()
        print(
            "The formatter will use the mygene API (%s) to convert gene ids."
            % self._database_url
        )

    @property
    def ontology_links(self) -> Dict[str, str]:
        return {
            "ENTREZ": "https://www.ncbi.nlm.nih.gov/gene/",
            "ENSEMBL": "http://useast.ensembl.org/index.html",
            "HGNC": "https://www.genenames.org",
            "SYMBOL": "https://www.genenames.org",
        }

    def _format_response(
        self, search_results: pd.DataFrame, batch_ids: List[str]
    ) -> None:
        """Format the response from the mygene API.

        Args:
            response (dict): The response from the mygene API. It was generated by the resp.json() method.
            batch_ids (List[str]): The list of ids for the current batch.

        Raises:
            Exception: If no results found.

        Returns:
            None
        """
        if search_results.empty:
            raise NoResultException()

        print(
            "Batch size: %s, results size: %s"
            % (len(batch_ids), search_results.shape[0])
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

            result = search_results.iloc[index].to_dict()
            converted_id_dict = {}
            converted_id_dict[prefix] = id
            converted_id_dict["raw_id"] = id
            difference = [x for x in self.databases if x != prefix]
            for choice in difference:
                matched = result.get(choice, None)
                if matched:
                    converted_id_dict[choice] = matched
                    converted_id_dict["idx"] = index

                    if choice == self.default_database and len(matched) > 1:
                        failed_id = FailedId(
                            idx=index, id=id, reason="Multiple results found"
                        )
                        self.add_failed_id(failed_id)
                        # Abandon the converted_id_dict, otherwise the converted_ids will be added to the converted_ids list.
                        converted_id_dict = {}
                        break

                    if self._strategy == Strategy.UNIQUE and len(matched) > 1:
                        failed_id = FailedId(
                            idx=index,
                            id=id,
                            reason="The strategy is unique, but multiple results found",
                        )
                        self.add_failed_id(failed_id)
                        # Abandon the converted_id_dict, otherwise the converted_ids will be added to the converted_ids list.
                        converted_id_dict = {}
                        break
                else:
                    converted_id_dict[choice] = None

            if converted_id_dict:
                self.add_converted_id(converted_id_dict)

    def _fetch_ids(self, ids) -> pd.DataFrame:
        """Fetch the ids from the mygene API.

        Args:
            ids (List[str]): A list of ids.

        Returns:
            dict: The response from the OXO API which was generated by the resp.json() method.
        """
        id_lst = [
            [id.split(":")[0], id.split(":")[1], idx] for (idx, id) in enumerate(ids)
        ]
        # Group the ids by the prefix.
        id_dict = {}
        id_idx_dict = {}
        for id in id_lst:
            if id[0] not in id_dict:
                id_dict[id[0]] = []
            id_dict[id[0]].append(id[1])
            # The id maybe same, such as HGNC:1 and ENTREZ:1. So we need to use full id as the key.
            id_idx_dict[f"{id[0]:id[1]}"] = id[2]

        groups = id_dict.keys()
        all_results = []
        for group in groups:
            ids = id_dict[group]
            scope = get_scope(group)
            default_fields = list(default_field_dict.values()) + additional_fields
            results = self._mygene.getgenes(
                ids, fields=",".join(default_fields), as_dataframe=True, scopes=scope
            )
            # MyGene will return the following columns:
            # _id,_version,entrezgene,name,symbol,taxid,ensembl.gene,HGNC

            # We need to keep the original order for matching the row number of user's input file.
            results["idx"] = results["_id"].apply(lambda x: id_idx_dict[f"{group}:{x}"])

            # Add the prefix to the id for the following processing.
            results["id"] = results["_id"].apply(lambda x: f"{group}:{x}")
            all_results.append(results)

        all_results = pd.concat(all_results).sort_values(by="idx", ascending=True)
        # Reverse the dictionary.
        fields = {v: k for k, v in default_field_dict.items()}

        # Rename the columns to match the database names, such as entrezgene to ENTREZ, ensembl.gene to ENSEMBL etc.
        all_results.rename(columns=fields, inplace=True)
        return all_results

    def convert(self) -> ConversionResult:
        """Convert the ids to different databases.

        Returns:
            ConversionResult: The results of id conversion.
        """
        # Cannot use the parallel processing, otherwise the index order will not be correct.
        for i in range(0, len(self.ids), self.batch_size):
            batch_ids = self.ids[i : i + self.batch_size]
            response = self._fetch_ids(batch_ids)
            self._format_response(response, batch_ids)
            time.sleep(self.sleep_time)

        return ConversionResult(
            ids=self.ids,
            strategy=self.strategy,
            converted_ids=self.converted_ids,
            databases=self.databases,
            default_database=self.default_database,
            database_url=self._database_url,
            failed_ids=self.failed_ids,
        )


class GeneOntologyFormatter(BaseOntologyFormatter):
    """Format the gene ontology file."""

    def __init__(
        self,
        filepath: Union[str, Path],
        dict: Optional[ConversionResult] = None,
        **kwargs,
    ) -> None:
        """Initialize the GeneOntologyFormatter class.

        Args:
            filepath (Union[str, Path]): The path of the gene ontology file. Only support csv and tsv file.
            dict (ConversionResult, optional): The results of id conversion. Defaults to None.
            **kwargs: The keyword arguments for the Gene class.
        """
        super().__init__(
            filepath,
            file_format_cls=GeneOntologyFileFormat,
            ontology_converter=GeneOntologyConverter,
            dict=dict,
            ontology_type=GENE_DICT,
            **kwargs,
        )

    def format(self):
        """Format the gene ontology file.

        Returns:
            self: The GeneOntologyFormatter instance.
        """
        formated_data = []
        failed_formatted_data = []

        for converted_id in self._dict.converted_ids:
            raw_id = converted_id.get("raw_id")
            id = converted_id.get(self.ontology_type.default)
            record = self.get_raw_record(raw_id)
            columns = self._expected_columns + self._optional_columns
            new_row = {
                key: self.format_record_value(record, key)
                for key in columns
            }

            if id is None:
                # Keep the original record if the id does not match the default prefix.
                unique_ids = self.get_alias_ids(converted_id)
                new_row["xrefs"] = "|".join(unique_ids)
                formated_data.append(new_row)
            elif type(id) == list and len(id) > 1:
                new_row["xrefs"] = "|".join(id)
                new_row["reason"] = "Multiple results found"
                failed_formatted_data.append(new_row)
            else:
                if type(id) == list and len(id) == 1:
                    id = id[0]

                new_row["raw_id"] = raw_id
                new_row[self.file_format_cls.ID] = id
                new_row[self.file_format_cls.RESOURCE] = self.ontology_type.default
                new_row[self.file_format_cls.LABEL] = self.ontology_type.type

                unique_ids = self.get_alias_ids(converted_id)
                new_row["xrefs"] = "|".join(unique_ids)

                formated_data.append(new_row)

        for failed_id in self._dict.failed_ids:
            id = failed_id.id
            prefix, value = id.split(":")
            record = self.get_raw_record(id)
            columns = self._expected_columns.extend(self._optional_columns)
            new_row = {
                key: self.format_record_value(record, key)
                for key in columns
            }
            new_row[self.file_format_cls.ID] = id
            new_row[self.file_format_cls.LABEL] = self.ontology_type.type
            new_row[self.file_format_cls.RESOURCE] = prefix
            new_row["xrefs"] = ""

            # Keep the original record if the id match the default prefix.
            if prefix == self.ontology_type.default or self._dict.strategy == Strategy.MIXTURE:
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
    ids = ["ENTREZ:7157", "ENTREZ:7158", "ENSEMBL:ENSG00000141510", "HGNC:11892"]
    gene = GeneOntologyConverter(ids)
    result = gene.convert()
    print(result)
