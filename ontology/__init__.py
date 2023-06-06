from typing import Type
from ontology.disease import DiseaseOntologyFormatter
from ontology.ontology import BaseOntologyFormatter

ontology_dict: dict[str, Type[BaseOntologyFormatter]] = {
    "disease": DiseaseOntologyFormatter,
    # "gene": None,
    # "compound": None,
    # "anatomy": None,
    # "pathway": None,
    # "cellular_component": None,
    # "molecular_function": None,
    # "biological_process": None,
    # "pharmacologic_class": None,
    # "side_effect": None,
    # "symptom": None,
    # "protein": None,
    # "metabolite": None,
}


__all__ = ["ontology_dict", "BaseOntologyFormatter", "DiseaseOntologyFormatter"]
