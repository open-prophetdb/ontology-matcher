import re
import json
import logging
import pandas as pd
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Union, Optional, Type, Any
from enum import Enum
from pathlib import Path

logger = logging.getLogger("ontology_matcher.ontology_formatter")


@dataclass
class OntologyType:
    type: str
    default: str
    choices: List[str]

    @property
    def ontology_type(self) -> str:
        return self.type

    @property
    def default_database(self) -> str:
        return self.default

    @property
    def databases(self) -> List[str]:
        return self.choices


class Strategy(Enum):
    UNIQUE = "Unique"
    MIXTURE = "Mixture"


@dataclass
class FailedId:
    idx: int
    id: str
    reason: str


@dataclass
class ConvertedId:
    idx: int
    raw_id: str
    metadata: Dict[str, str] | None

    def _getattr(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError:
            return None

    @classmethod
    def from_args(cls, **kwargs):
        try:
            initializer = cls.__initializer
        except AttributeError:
            # Store the original init on the class in a different place
            cls.__initializer = initializer = cls.__init__
            # replace init with something harmless
            cls.__init__ = lambda *a, **k: None

        # code from adapted from Arne
        added_args = {}
        for name in list(kwargs.keys()):
            if name not in cls.__annotations__:
                added_args[name] = kwargs.pop(name)

        ret = object.__new__(cls)
        initializer(ret, **kwargs)  # type: ignore
        # ... and add the new ones by hand
        for new_name, new_val in added_args.items():
            setattr(ret, new_name, new_val)

        return ret

    def update_metadata(self, metadata: Dict[str, str]):
        for key in metadata.keys():
            if not self.metadata:
                self.metadata = {}

            self.metadata[key] = metadata[key]

    def get_idx(self) -> int:
        return self._getattr("idx")

    def get_raw_id(self) -> str:
        return self._getattr("raw_id")

    def get_metadata(self) -> Dict[str, str] | None:
        return self._getattr("metadata")

    def get(self, key: str) -> Any:
        func = self._getattr("get_%s" % key)
        if func:
            return func()
        else:
            return self._getattr(key)


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ConversionResult):
            return {
                "ids": obj.ids,
                "strategy": "Mixture" if obj.strategy == Strategy.MIXTURE else "Unique",
                "default_database": obj.default_database,
                "converted_ids": obj.converted_ids,
                "databases": obj.databases,
                "database_url": obj.database_url,
                "failed_ids": obj.failed_ids,
            }
        elif isinstance(obj, ConvertedId):
            return obj.__dict__
        elif isinstance(obj, FailedId):
            return obj.__dict__
        elif isinstance(obj, pd.DataFrame):
            return obj.to_dict()

        return super().default(obj)


class CustomJSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        if (
            "conversion_result" in obj
            and "formatted_data" in obj
            and "failed_formatted_data" in obj
            and "filepath" in obj
            and "data" in obj
        ):
            data = pd.DataFrame.from_dict(obj["data"])
            formatted_data = pd.DataFrame.from_dict(obj["formatted_data"])
            failed_formatted_data = pd.DataFrame.from_dict(obj["failed_formatted_data"])
            filepath = obj["filepath"]
            cr = obj["conversion_result"]

            return {
                "conversion_result": ConversionResult(
                    ids=cr["ids"],
                    strategy=Strategy.MIXTURE
                    if cr["strategy"] == "Mixture"
                    else Strategy.UNIQUE,
                    default_database=cr["default_database"],
                    converted_ids=cr["converted_ids"],
                    databases=cr["databases"],
                    database_url=cr["database_url"],
                    failed_ids=cr["failed_ids"],
                ),
                "formatted_data": formatted_data,
                "failed_formatted_data": failed_formatted_data,
                "filepath": filepath,
                "data": data,
            }
        elif "idx" in obj and "raw_id" in obj and "metadata" in obj:
            return ConvertedId.from_args(**obj)
        elif "idx" in obj and "id" in obj and "reason" in obj:
            return FailedId(
                idx=obj["idx"],
                id=obj["id"],
                reason=obj["reason"],
            )

        return obj


@dataclass
class GroupedIds:
    id_dict: Dict[str, List[str]]
    id_idx_dict: Dict[str, int]


def make_grouped_ids(ids: List[str]) -> GroupedIds:
    """Make grouped ids.

    Args:
        ids (List[str]): A set of ids, each id should be in the format of <database>:<id>.

    Returns:
        `GroupedIds`: The grouped ids which have two dictionaries: id_dict and id_idx_dict. The id_dict is used to group the ids by the prefix. The id_idx_dict is used to store the index of the id in the original list. id_dict: Group the ids by the prefix. such as `{'ENTREZ': ['7157', '7158'], 'ENSEMBL': ['ENSG00000141510'], 'HGNC': ['11892']}`; id_idx_dict: Store the index of the id in the original list. such as `{'ENTREZ:7157': 0, 'ENTREZ:7158': 1, 'ENSEMBL:ENSG00000141510': 2, 'HGNC:11892': 3}`
    """
    id_lst = [[id.split(":")[0], id.split(":")[1], idx] for (idx, id) in enumerate(ids)]

    id_dict: Dict[str, List[str]] = {}
    id_idx_dict: Dict[str, int] = {}
    for id in id_lst:
        if id[0] not in id_dict:
            id_dict[id[0]] = []
        id_dict[id[0]].append(id[1])
        # The id maybe same, such as HGNC:1 and ENTREZ:1. So we need to use full id as the key and the related index as the value for indexing and reordering the results.
        id_idx_dict[f"{id[0]}:{id[1]}"] = id[2]

    return GroupedIds(id_dict, id_idx_dict)


def flatten_dedup(nested_list: List[List[str]] | List[List[str] | str] | List[str]) -> List[str]:
    flat_list = []
    for sublist in nested_list:
        if isinstance(sublist, list):
            flat_list.extend(sublist)
        else:
            flat_list.append(sublist)
    return list(set(flat_list))


@dataclass
class ConversionResult:
    ids: List[str]
    strategy: Strategy
    default_database: str
    converted_ids: List[ConvertedId]
    databases: List[str]
    database_url: Optional[str]
    failed_ids: List[FailedId]


class OntologyBaseConverter:
    """Base class for ontology converters."""

    def __init__(
        self,
        ontology_type: OntologyType,
        ids: List[str],
        strategy=Strategy.MIXTURE,
        batch_size: int = 300,
        sleep_time: int = 3,
    ) -> None:
        """Initialize the ontology converter.

        Args:
            ontology_type (OntologyType): The ontology type.
            ids (List[str]): The list of ids to be converted.
            strategy (Strategy, optional): The strategy to be used. Defaults to Strategy.MIXTURE.
            batch_size (int, optional): The batch size. Defaults to 300.
            sleep_time (int, optional): The sleep time. Defaults to 3.

        Raises:
            Exception: If the batch size is larger than 500.
            Exception: If the ids are not in the correct format.
        """
        # Remove nan values
        self._ids = list(filter(lambda x: x and isinstance(x, str), ids))
        self._strategy = strategy
        self._default_database = ontology_type.default
        self._failed_ids: List[FailedId] = []
        self._converted_ids: List[ConvertedId] = []
        self._databases = ontology_type.choices
        self._batch_size = batch_size
        self._sleep_time = sleep_time

        self.print_ontology_links()
        self.check_batch_size()
        self._check_ids()

    def default_check_batch_size(self):
        """Check the batch size.

        Raises:
            Exception: If the batch size is larger than 500.
        """
        if self._batch_size > 500:
            raise Exception("The batch size cannot be larger than 500.")
        
    def check_batch_size(self):
        """Check the batch size."""
        raise NotImplementedError

    @property
    def ontology_links(self) -> Dict[str, str]:
        return NotImplementedError  # type: ignore

    def _check_ids(self):
        """Check if the ids are in the correct format.

        Raises:
            Exception: If the ids are not in the correct format.
        """
        failed_ids = []
        for idx, id in enumerate(self._ids):
            if not isinstance(id, str):
                failed_ids.append(
                    {"idx": idx, "id": id, "reason": "The id must be a string."}
                )

            if not re.match(r"^(%s):[a-z0-9A-Z\.]+$" % "|".join(self._databases), id):
                failed_ids.append(
                    {
                        "idx": idx,
                        "id": id,
                        "reason": "The id must be in the format of <database>:<id>. Only support the following databases: %s. Besides, the id must match the pattern [a-z0-9A-Z.]+"
                        % self._databases,
                    }
                )

        if len(failed_ids) > 0:
            raise Exception(failed_ids)

    @property
    def ids(self):
        return self._ids

    @property
    def strategy(self):
        return self._strategy

    @property
    def default_database(self):
        return self._default_database

    @property
    def failed_ids(self):
        return self._failed_ids

    @property
    def converted_ids(self) -> List[ConvertedId]:
        return self._converted_ids

    @property
    def databases(self):
        return self._databases

    @property
    def batch_size(self):
        return self._batch_size

    @property
    def sleep_time(self):
        return self._sleep_time

    def add_failed_id(self, failed_id: FailedId):
        """Add a failed id into the list of failed ids."""
        self._failed_ids.append(failed_id)

    def add_failed_ids(self, failed_ids: List[FailedId]):
        """Add a list of failed ids."""
        for failed_id in failed_ids:
            self.add_failed_id(failed_id)

    def print_ontology_links(self):
        """Print the ontology links."""
        # Check the ontology links
        missed = set(self._databases) - set(self.ontology_links.keys())
        if len(missed) > 0:
            raise Exception("Links of the following databases are missed: %s" % missed)

        print(
            "NOTICE:\nYou can find more details on the following websites (NOTICE: We don't check whether an ID is valid; we simply attempt to map it to the default ontology database we have chosen):"
        )
        for key, value in self.ontology_links.items():
            print(f"{key}: {value}")

        print("\n")

    def add_converted_id_dict(self, converted_id_dict: Dict[str, Any]):
        """Add a converted id into the list of converted ids."""
        self._converted_ids.append(ConvertedId.from_args(**converted_id_dict))

    def add_converted_ids(self, converted_ids: List[ConvertedId]):
        self._converted_ids.extend(converted_ids)

    def add_converted_id_dicts(self, converted_id_dicts: List[Dict[str, Any]]):
        """Add a list of converted ids."""
        for converted_id_dict in converted_id_dicts:
            self.add_converted_id_dict(converted_id_dict)

    def id_dicts2converted_ids(self, id_dicts: List[Dict[str, List[str]]]) -> List[ConvertedId]:
        """Convert the id dicts to converted ids.

        Args:
            id_dicts (List[Dict[str, List[str]]]): The id dicts.

        Returns:
            List[ConvertedId]: The converted ids.
        """
        converted_ids = []
        for id_dict in id_dicts:
            converted_id = ConvertedId.from_args(**id_dict)
            converted_ids.append(converted_id)

        return converted_ids

    def convert(self) -> ConversionResult:
        """Convert the ids.

        Returns:
            ConversionResult: The conversion result.
        """
        raise NotImplementedError


class BaseOntologyFileFormat:
    ID = "id"
    NAME = "name"
    LABEL = "label"
    RESOURCE = "resource"
    DESCRIPTION = "description"
    SYNONYMS = "synonyms"
    PMIDS = "pmids"
    TAXID = "taxid"
    XREFS = "xrefs"

    @classmethod
    def expected_columns(cls) -> List[str]:
        """Get the expected columns.

        Returns:
            List[str]: The expected columns.
        """
        return [
            cls.ID,
            cls.NAME,
            cls.LABEL,
            cls.RESOURCE,
        ]

    @classmethod
    def optional_columns(cls) -> List[str]:
        """Get the optional columns.

        Returns:
            List[str]: The optional columns.
        """
        return [
            cls.DESCRIPTION,
            cls.SYNONYMS,
            cls.PMIDS,
            cls.TAXID,
            cls.XREFS,
        ]

    @classmethod
    def generate_template(cls, filepath: Union[str, Path]):
        """Generate a template file.

        Args:
            filepath (Union[str, Path]): The path of the template file.
        """
        raise NotImplementedError


class BaseOntologyFormatter(ABC):
    """Format the base ontology file."""

    def __init__(
        self,
        filepath: Union[str, Path],
        file_format_cls: Optional[Type[BaseOntologyFileFormat]] = None,
        ontology_type: Optional[OntologyType] = None,
        ontology_converter: Optional[Type[OntologyBaseConverter]] = None,
        conversion_result: Optional[ConversionResult] = None,
        **kwargs,
    ) -> None:
        """Initialize the DiseaseOntologyFormatter class.

        Args:
            filepath (Union[str, Path]): The path of the disease ontology file. Only support csv and tsv file.
            conversion_result (ConversionResult, optional): The results of id conversion. Defaults to None.
            **kwargs: The keyword arguments for the Disease class.
        """
        if not ontology_type:
            raise Exception("The ontology type must be specified.")
        else:
            self.ontology_type: OntologyType = ontology_type  # type: ignore

        if not file_format_cls:
            raise Exception("The format_cls must be specified.")
        else:
            self.file_format_cls: Type[BaseOntologyFileFormat] = file_format_cls  # type: ignore

        if not ontology_converter:
            raise Exception("The ontology converter must be specified.")

        # Using a class method to get the expected columns will be more robust.
        self._expected_columns = self.file_format_cls.expected_columns()

        self._optional_columns = self.file_format_cls.optional_columns()

        # self._expected_columns = [
        #     getattr(self.file_format_cls, attr)
        #     for attr in dir(self.file_format_cls)
        #     if not attr.startswith("__") and not callable(getattr(self.file_format_cls, attr))
        # ]

        self._filepath = filepath
        self._data = self._read_file()
        self._formatted_data = None
        self._failed_formatted_data = None

        self._check_format()

        all_ids = self._data[self.file_format_cls.ID].tolist()

        logger.info(f"Total number of IDs: {len(all_ids)}")
        if conversion_result is None:
            self._conversion_result = ontology_converter(
                ids=all_ids, **kwargs
            ).convert()
        else:
            self._conversion_result = conversion_result

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @property
    def formatted_data(self) -> Optional[pd.DataFrame]:
        return self._formatted_data

    @property
    def failed_formatted_data(self) -> Optional[pd.DataFrame]:
        return self._failed_formatted_data

    @property
    def conversion_result(self) -> ConversionResult:
        return self._conversion_result

    @property
    def filepath(self) -> Union[str, Path]:
        return self._filepath

    def _read_file(self) -> pd.DataFrame:
        """Read the disease ontology file.

        Returns:
            pd.DataFrame: The disease ontology data.
        """
        path = Path(self._filepath)
        ext = path.suffix.strip(".")
        delimiter = "," if ext == "csv" else "\t"
        data = pd.read_csv(path, delimiter=delimiter)
        # Remove the nan values
        data = data[data[self.file_format_cls.ID].notna()]
        data.fillna("", inplace=True)

        return data

    def join_lst(self, lst: List[str] | str) -> str:
        if isinstance(lst, str):
            lst = list(set(lst.split("|")))
            return "|".join(filter(lambda x: x, lst))
        elif isinstance(lst, list):
            new_lst = [x.split("|") for x in lst if x]
            lst = flatten_dedup(new_lst)
            return "|".join(filter(lambda x: x, lst))
        else:
            return ""

    def concat(self, x: str | List[str], y: str | List[str]) -> List[str]:
        if x:
            if isinstance(x, str):
                x = [x]
        else:
            x = []

        if y:
            if isinstance(y, str):
                y = [y]
        else:
            y = []

        return list(set(x + y))

    def _check_format(self) -> bool:
        """Check the format of the disease ontology file.

        Returns:
            bool: True if the format is correct, otherwise raise an exception.
        """
        columns = self._data.columns.tolist()
        missed_columns = []
        for col in self._expected_columns:
            if col not in columns:
                missed_columns.append(col)

        if len(missed_columns) > 0:
            raise Exception(
                "The file format is not correct, missed columns: %s"
                % ", ".join(missed_columns)
            )
        return True

    def get_raw_record(self, id: str) -> pd.DataFrame:
        """Get the raw record by id.

        Args:
            id (str): The id of the record.

        Returns:
            pd.DataFrame: The raw record.
        """
        records = self._data[self._data[self.file_format_cls.ID] == id]
        logger.debug("Get the raw record: %s" % records)
        if len(records) == 0:
            raise ValueError(
                "Cannot find the related record, please check your id. you may need to use the raw id not the converted id."
            )
        elif len(records) > 1:
            return pd.DataFrame(records[0])
        else:
            return records

    @staticmethod
    def format_record_value(record: pd.DataFrame, key: str) -> Any:
        """Format the record value.

        Args:
            record (pd.DataFrame): The record.
            key (str): The key of the record.

        Returns:
            str: The formatted record value.
        """
        try:
            return record[key].values[0]
        except KeyError:
            return ""

    def get_alias_ids(self, converted_id: ConvertedId) -> List[str]:
        ids = [
            converted_id.get(x)
            for x in self.ontology_type.choices
            if x != self.ontology_type.default
        ]
        unique_ids = []
        for id in ids:
            if type(id) == list:
                unique_ids.extend(id)
            elif type(id) == str and id not in unique_ids:
                unique_ids.append(id)

        # Remove the empty ids
        filtered_ids = filter(lambda x: x, unique_ids)

        return list(set(filtered_ids))

    def format(self):
        """Format the disease ontology file.

        Returns:
            self: The OntologyFormatter instance.
        """
        raise NotImplementedError
    
    def format_by_metadata(
        self, new_row: Dict[str, Any], metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        raise NotImplementedError

    def default_format_by_metadata(
        self, new_row: Dict[str, Any], metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Format the row by metadata. If you want to add more columns, you can override this method."""
        new_row[self.file_format_cls.NAME] = metadata.get("name") or new_row.get("name")
        new_row[self.file_format_cls.DESCRIPTION] = metadata.get(
            "description"
        ) or new_row.get("description")
        new_row[self.file_format_cls.SYNONYMS] = self.concat(
            metadata.get("synonyms", []), new_row.get("synonyms", [])
        )
        new_row[self.file_format_cls.XREFS] = self.concat(
            metadata.get("xrefs", []), new_row.get("xrefs", [])
        )
        new_row[self.file_format_cls.PMIDS] = self.concat(
            metadata.get("pmids", []), new_row.get("pmids", [])
        )
        return new_row

    def default_format(self):
        """Format the ontology file. If you don't any specified format, you can use this method to format the ontology file. For example, you implement the format method and call this method in the format method.

        Returns:
            self: The OntologyFormatter instance.
        """
        formated_data = []
        failed_formatted_data = []

        total = len(self.conversion_result.converted_ids)
        logger.info(
            "Start formatting the disease ontology file, which contains %s rows."
            % total
        )
        for index, converted_id in enumerate(self.conversion_result.converted_ids):
            logger.info("Processing %s/%s" % (index + 1, total))
            raw_id = converted_id.get("raw_id")
            id = converted_id.get(self.ontology_type.default)
            record = self.get_raw_record(raw_id)
            columns = self._expected_columns + self._optional_columns
            new_row = {key: self.format_record_value(record, key) for key in columns}

            metadata = converted_id.get_metadata()

            if metadata:
                new_row = self.format_by_metadata(new_row, metadata)

            # Keep the original record if the id does not match the default prefix.
            unique_ids = self.get_alias_ids(converted_id)
            xrefs = self.concat(unique_ids, new_row.get(self.file_format_cls.XREFS, []))

            synonyms = new_row.get(self.file_format_cls.SYNONYMS, record.get("synonyms"))
            new_row[self.file_format_cls.SYNONYMS] = self.join_lst(synonyms)

            pmids = new_row.get(self.file_format_cls.PMIDS, record.get("pmids"))
            new_row[self.file_format_cls.PMIDS] = self.join_lst(pmids)

            if id is None:
                new_row[self.file_format_cls.ID] = raw_id
                new_row[self.file_format_cls.XREFS] = self.join_lst(xrefs)
                formated_data.append(new_row)
                logger.debug("No results found for %s, %s" % (raw_id, new_row))
            elif type(id) == list and len(id) > 1:
                new_row[self.file_format_cls.XREFS] = self.join_lst(
                    self.concat(id, xrefs)
                )
                new_row["reason"] = "Multiple results found"
                failed_formatted_data.append(new_row)
            else:
                if type(id) == list:
                    if len(id) == 1:
                        id = id[0]
                    else:
                        id = raw_id

                new_row["raw_id"] = raw_id
                new_row[self.file_format_cls.ID] = str(id)
                new_row[self.file_format_cls.RESOURCE] = self.ontology_type.default
                new_row[self.file_format_cls.LABEL] = self.ontology_type.type

                new_row[self.file_format_cls.XREFS] = self.join_lst(xrefs)

                formated_data.append(new_row)

        total = len(self.conversion_result.failed_ids)
        logger.info("Start formatting the failed ids, which contains %s rows." % total)
        for index, failed_id in enumerate(self.conversion_result.failed_ids):
            logger.info("[Failed ID] Processing %s/%s" % (index + 1, total))
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

    def filter(self) -> pd.DataFrame:
        """Filter the invalid data."""
        raise NotImplementedError

    def save_to_json(self, filepath: Union[str, Path]):
        obj = {
            "conversion_result": self.conversion_result,
            "formatted_data": self._formatted_data,
            "failed_formatted_data": self._failed_formatted_data,
            "filepath": self._filepath,
            "data": self._data,
        }

        # Save the object
        json_file = Path(filepath).with_suffix(".json")
        if not json_file.exists():
            with open(json_file, "w") as f:
                json.dump(obj, f, cls=CustomJSONEncoder)

    def write(self, filepath: Union[str, Path]):
        """Write the formatted data to the file.

        Args:
            filepath (Union[str, Path]): The file path to write the formatted data. The file extension should be .tsv. Three files will be generated: the formatted data, the failed formatted data and the json file.
        """
        # Check the directory whether exists, if not, create it.
        if not Path(filepath).parent.exists():
            Path(filepath).parent.mkdir(parents=True)

        if self._formatted_data is None:
            raise Exception(
                "Cannot find the valid formatted data, maybe the format method is not called or the formatted data is empty (please check the failed_formatted_data attributes)."
            )

        if self._formatted_data is not None:
            self._formatted_data.to_csv(filepath, sep="\t", index=False)

        if self._failed_formatted_data is not None:
            self._failed_formatted_data.to_csv(
                Path(filepath).with_suffix(".failed.tsv"), sep="\t", index=False
            )

        self.save_to_json(filepath)


class NoResultException(Exception):
    """The exception for no result."""

    def __init__(self, message: str = "") -> None:
        """Initialize the NoResultException class.

        Args:
            message (str, optional): The error message.
        """
        if message == "":
            message = "Sorry, something went wrong. No results found. It may be caused by network issue, traffic limit or invalid ids. If the issue persists, you can choose a big sleep time and a small batch size, then try again. or check the ids manually."
        super().__init__(message)
