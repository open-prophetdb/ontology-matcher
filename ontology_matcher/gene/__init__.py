import time
import logging
from ontology_matcher.apis import MyGene
import pandas as pd
from pathlib import Path
from typing import Union, List, Optional, Dict, Any
from ontology_matcher.ontology_formatter import (
    OntologyType,
    Strategy,
    ConversionResult,
    FailedId,
    OntologyBaseConverter,
    BaseOntologyFormatter,
    NoResultException,
    make_grouped_ids,
    flatten_dedup,
)
from ontology_matcher.gene.custom_types import GeneOntologyFileFormat

logger = logging.getLogger("ontology_matcher.gene")

default_field_dict = {
    "ENTREZ": "entrezgene",
    "ENSEMBL": "ensembl.gene",
    "HGNC": "HGNC",
    "MGI": "MGI",
    "SYMBOL": "symbol",
    "UNIPROT": "uniprot.Swiss-Prot",
}

# Get the following fields from the mygene API. Just for getting more information.
additional_fields = [
    "name",
    "taxid",
    "alias",
    "summary",
    "other_names",
    "uniport.Swiss-Prot",
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
        logger.info(
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
            "MGI": "http://www.informatics.jax.org",
            "UNIPROT": "https://www.uniprot.org/uniprot/",
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

        def list_or_str(x):
            x = list(set(x))
            if type(x) == list and len(x) == 1:
                return x[0]
            else:
                return x

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

            logger.debug("Processing %s" % search_results)
            # The returned MGI ids are like MGI:1342288, so we need to use the full id to match the results.
            # Other ids are like 7157 for ENTREZ, so we need to use the value to match the results.
            if prefix == "MGI":
                result = search_results[search_results[prefix] == id]
            else:
                result = search_results[search_results[prefix] == value]

            # If we cannot find information for the id, this means that the id is not valid. So we don't need to check the result for the default database.
            if result.empty:
                failed_id = FailedId(idx=index, id=id, reason="No results found")
                self.add_failed_id(failed_id)
                continue

            converted_id_dict = {}
            converted_id_dict[prefix] = id
            converted_id_dict["raw_id"] = id
            converted_id_dict["metadata"] = (
                result.to_dict(orient="records")[0] if result.empty is False else None
            )

            difference = [x for x in self.databases if x != prefix]
            for choice in difference:
                try:
                    matched = flatten_dedup(result.loc[:, choice].tolist())
                except KeyError:
                    matched = None

                if matched:
                    converted_id_dict[choice] = list_or_str(
                        map(
                            lambda x: f"{choice}:{x}"
                            if choice != "MGI" and x is not None
                            else x,
                            matched,
                        )
                    )
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
                    # Why we don't abandon the result of the default database if it is empty?
                    # Because we would like to convert ids to the default database as much as possible.
                    # But also need to keep ids from multiple resources for gaining more entities.
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
        grouped_ids = make_grouped_ids(ids)
        id_dict = grouped_ids.id_dict
        id_idx_dict = grouped_ids.id_idx_dict

        # Groups may be similar to ['ENTREZ', 'ENSEMBL', 'HGNC']
        groups = id_dict.keys()

        # Check all scopes are valid.
        for scope in id_dict.keys():
            if scope not in self.databases:
                raise ValueError("Invalid prefix, only support %s" % self.databases)

        all_results = []
        for group in groups:
            ids = id_dict[group]
            scope = get_scope(group)
            # All fields information: http://mygene.info/v3/gene/1017
            default_fields = list(default_field_dict.values()) + additional_fields

            request = MyGene(
                q=",".join(ids), scopes=scope, fields=default_fields, dotfield=True
            )
            results = request.parse()

            results = pd.DataFrame(results)
            # We don't like nan, so we need to convert it to None.
            results = results.where(pd.notnull(results), None)

            # MyGene will return the following columns:
            # _id,_version,entrezgene,name,symbol,taxid,ensembl.gene,HGNC,summary,alias,query,MGI,other_names

            # We need to keep the original order for matching the row number of user's input file.
            if scope == "MGI":
                # The MGI id should be MGI:MGI:1342288, so we need to remove the prefix.
                results["query"] = results["query"].apply(lambda x: x.split(":")[1])

            results["idx"] = results["query"].apply(
                lambda x: id_idx_dict[f"{group}:{x}"]
            )

            # Add the prefix to the id for the following processing.
            results["id"] = results["query"].apply(lambda x: f"{group}:{x}")
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

            total = len(self.ids)
            c = i + self.batch_size if i + self.batch_size < total else total
            logger.info("Finish %s/%s" % (c, len(self.ids)))
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
        conversion_result: Optional[ConversionResult] = None,
        **kwargs,
    ) -> None:
        """Initialize the GeneOntologyFormatter class.

        Args:
            filepath (Union[str, Path]): The path of the gene ontology file. Only support csv and tsv file.
            conversion_result (ConversionResult, optional): The results of id conversion. Defaults to None.
            **kwargs: The keyword arguments for the Gene class.
        """
        super().__init__(
            filepath,
            file_format_cls=GeneOntologyFileFormat,
            ontology_converter=GeneOntologyConverter,
            conversion_result=conversion_result,
            ontology_type=GENE_DICT,
            **kwargs,
        )

    def format_synonyms(
        self,
        alias: List[str] | float | str | None,
        other_names: List[str] | float | str | None,
    ) -> List[str]:
        synonyms = []
        if type(alias) == str:
            synonyms.append(alias)

        if type(other_names) == str:
            synonyms.append(other_names)

        if type(alias) == list:
            synonyms.extend(alias)

        if type(other_names) == list:
            synonyms.extend(other_names)

        synonyms = list(set(synonyms))
        return synonyms

    def format_by_metadata(
        self, new_row: Dict[str, Any], metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Format the row by the metadata."""
        new_row = self.default_format_by_metadata(new_row, metadata)

        symbol = metadata.get("Symbol")
        # If the symbol is not available, use the raw name instead.
        if symbol:
            new_row[self.file_format_cls.NAME] = symbol

        new_row[self.file_format_cls.TAXID] = metadata.get("taxid")

        new_row[self.file_format_cls.DESCRIPTION] = metadata.get("summary")

        alias = metadata.get("alias")
        other_names = metadata.get("other_names")
        synonyms = self.format_synonyms(alias, other_names).append(
            metadata.get("name", "")
        )
        synonyms = self.concat(new_row.get("synonyms", []), synonyms or [])

        new_row[self.file_format_cls.SYNONYMS] = synonyms
        return new_row

    def format(self):
        """Format the gene ontology file.

        Returns:
            self: The GeneOntologyFormatter instance.
        """
        self.default_format()


if __name__ == "__main__":
    ids = [
        "ENTREZ:7157",
        "ENTREZ:7158",
        "ENSEMBL:ENSG00000141510",
        "HGNC:11892",
        "SYMBOL:NOTFOUND",
        "HGNC:NOTFOUND",
        "SYMBOL:TP53",
        "MGI:1342288",
    ]
    gene = GeneOntologyConverter(ids)
    result = gene.convert()
    print(result)

    example_file = Path(__file__).parent.parent.parent / "examples" / "gene.tsv"
    gene_formatter = GeneOntologyFormatter(example_file)
    gene_formatter.format()
    gene_formatter.write("/tmp/gene_output.tsv")
    print("You can find the output file at /tmp/gene_output.tsv")
