from typing import Type
from ontology_matcher.disease import DiseaseOntologyFormatter, DISEASE_DICT
from ontology_matcher.disease.custom_types import DiseaseOntologyFileFormat

from ontology_matcher.gene import GeneOntologyFormatter, GENE_DICT
from ontology_matcher.gene.custom_types import GeneOntologyFileFormat

from ontology_matcher.symptom import SymptomOntologyFormatter, SYMPTOM_DICT
from ontology_matcher.symptom.custom_types import SymptomOntologyFileFormat

from ontology_matcher.ontology_formatter import (
    BaseOntologyFormatter,
    BaseOntologyFileFormat,
    OntologyType,
)

ONTOLOGY_DICT: dict[str, Type[BaseOntologyFormatter]] = {
    "disease": DiseaseOntologyFormatter,
    "gene": GeneOntologyFormatter,
    # "compound": None,
    # "anatomy": None,
    # "pathway": None,
    # "cellular_component": None,
    # "molecular_function": None,
    # "biological_process": None,
    # "pharmacologic_class": None,
    # "side_effect": None,
    "symptom": SymptomOntologyFormatter,
    # "protein": None,
    # "metabolite": None,
}

ONTOLOGY_DICT_KEYS = list(ONTOLOGY_DICT.keys())

ONTOLOGY_TYPE_DICT: dict[str, OntologyType] = {
    "disease": DISEASE_DICT,
    "gene": GENE_DICT,
    # "compound": None,
    # "anatomy": None,
    # "pathway": None,
    # "cellular_component": None,
    # "molecular_function": None,
    # "biological_process": None,
    # "pharmacologic_class": None,
    # "side_effect": None,
    "symptom": SYMPTOM_DICT,
    # "protein": None,
    # "metabolite": None,
}

ONTOLOGY_FILE_FORMAT_DICT: dict[str, Type[BaseOntologyFileFormat]] = {
    "disease": DiseaseOntologyFileFormat,
    "gene": GeneOntologyFileFormat,
    # "compound": None,
    # "anatomy": None,
    # "pathway": None,
    # "cellular_component": None,
    # "molecular_function": None,
    # "biological_process": None,
    # "pharmacologic_class": None,
    # "side_effect": None,
    "symptom": SymptomOntologyFileFormat,
    # "protein": None,
    # "metabolite": None,
}

__all__ = [
    "ONTOLOGY_DICT",
    "BaseOntologyFormatter",
    "DiseaseOntologyFormatter",
    "GeneOntologyFormatter",
    "ONTOLOGY_DICT_KEYS",
    "ONTOLOGY_TYPE_DICT",
]
