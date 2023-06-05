import re
from dataclasses import dataclass
from typing import List, Dict, Union
from enum import Enum

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

@dataclass
class ConversionResult:
    ids: List[str]
    strategy: Strategy
    default_database: str
    converted_ids: List[Union[ConvertedId, Dict[str, str]]]
    databases: List[str]
    database_url: str
    failed_ids: List[FailedId]


class OntologyBaseConverter:
    """Base class for ontology converters."""
    def __init__(self, ontology_type: OntologyType, ids: List[str], strategy=Strategy.MIXTURE, 
                 batch_size: int=300, sleep_time: int=3) -> None:
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

        if self._batch_size > 500:
            raise Exception("The batch size cannot be larger than 500.")
        
        self._check_ids()
        
    def _check_ids(self):
        """Check if the ids are in the correct format.
        
        Raises:
            Exception: If the ids are not in the correct format.
        """
        failed_ids = []
        for (idx, id) in enumerate(self._ids):
            if not isinstance(id, str):
                failed_ids.append({
                    "idx": idx,
                    "id": id,
                    "reason": "The id must be a string."
                })
            
            if not re.match(r"^(%s):[a-z0-9A-Z]+$" % "|".join(self._databases), id):
                failed_ids.append({
                    "idx": idx,
                    "id": id,
                    "reason": "The id must be in the format of <database>:<id>."
                })

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
    
    def convert(self) -> ConversionResult:
        """Convert the ids.

        Returns:
            ConversionResult: The conversion result.
        """
        raise NotImplementedError
