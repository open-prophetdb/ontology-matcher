import re
import pickle
import pandas as pd
from dataclasses import dataclass
from typing import List, Dict, Union, Optional, Type
from enum import Enum
from pathlib import Path


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

    def get(self, key: str) -> Optional[Union[str, int]]:
        return getattr(self, key, None)


@dataclass
class ConversionResult:
    ids: List[str]
    strategy: Strategy
    default_database: str
    converted_ids: List[Union[ConvertedId, Dict[str, str]]]
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
        self._ids = ids
        self._strategy = strategy
        self._default_database = ontology_type.default
        self._failed_ids = []
        self._converted_ids = []
        self._databases = ontology_type.choices
        self._batch_size = batch_size
        self._sleep_time = sleep_time

        self.print_ontology_links()

        if self._batch_size > 500:
            raise Exception("The batch size cannot be larger than 500.")

        self._check_ids()

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
                        "reason": "The id must be in the format of <database>:<id>.",
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
    def converted_ids(self):
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

    def add_converted_id(self, converted_id: Union[ConvertedId, Dict[str, str]]):
        """Add a converted id into the list of converted ids."""
        self._converted_ids.append(converted_id)

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


class BaseOntologyFormatter:
    """Format the base ontology file."""

    def __init__(
        self,
        filepath: Union[str, Path],
        file_format_cls: Optional[Type[BaseOntologyFileFormat]] = None,
        ontology_converter: Optional[Type[OntologyBaseConverter]] = None,
        dict: Optional[ConversionResult] = None,
        ontology_type: Optional[OntologyType] = None,
        **kwargs,
    ) -> None:
        """Initialize the DiseaseOntologyFormatter class.

        Args:
            filepath (Union[str, Path]): The path of the disease ontology file. Only support csv and tsv file.
            dict (ConversionResult, optional): The results of id conversion. Defaults to None.
            **kwargs: The keyword arguments for the Disease class.
        """
        self._filepath = filepath
        self._data = self._read_file()
        self._formatted_data = None
        self._failed_formatted_data = None
        self.file_format_cls = file_format_cls
        self.ontology_type = ontology_type

        if self.file_format_cls is None:
            raise Exception("The format_cls must be specified.")

        if ontology_converter is None:
            raise Exception("The ontology converter must be specified.")
        
        # Using a class method to get the expected columns will be more robust.
        self._expected_columns = self.file_format_cls.expected_columns()

        self._optional_columns = self.file_format_cls.optional_columns()

        # self._expected_columns = [
        #     getattr(self.file_format_cls, attr)
        #     for attr in dir(self.file_format_cls)
        #     if not attr.startswith("__") and not callable(getattr(self.file_format_cls, attr))
        # ]

        self._check_format()

        all_ids = self._data[self.file_format_cls.ID].tolist()

        print(f"Total number of IDs: {len(all_ids)}")
        if dict is None:
            self._dict = ontology_converter(ids=all_ids, **kwargs).convert()
        else:
            self._dict = dict

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
    def dict(self) -> Optional[ConversionResult]:
        return self._dict

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
        return pd.read_csv(path, delimiter=delimiter)

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
        if len(records) == 0:
            raise ValueError(
                "Cannot find the related record, please check your id. you may need to use the raw id not the converted id."
            )
        elif len(records) > 1:
            return records[0]
        else:
            return records

    @staticmethod
    def format_record_value(record: pd.DataFrame, key: str) -> str:
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

        return unique_ids  

    def format(self):
        """Format the disease ontology file.

        Returns:
            self: The OntologyFormatter instance.
        """
        raise NotImplementedError

    def filter(self) -> pd.DataFrame:
        """Filter the invalid data."""
        raise NotImplementedError

    def write(self, filepath: Union[str, Path]):
        """Write the formatted data to the file.

        Args:
            filepath (Union[str, Path]): The file path to write the formatted data. The file extension should be .tsv. Three files will be generated: the formatted data, the failed formatted data and the pickle file.
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

        obj = {
            "dict": self._dict,
            "formatted_data": self._formatted_data,
            "failed_formatted_data": self._failed_formatted_data,
            "filepath": self._filepath,
            "data": self._data,
        }

        # Pickle the object
        with open(Path(filepath).with_suffix(".pkl"), "wb") as f:
            pickle.dump(obj, f)


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
