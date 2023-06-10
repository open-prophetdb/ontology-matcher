import re
import time
import requests
import pandas as pd
from pathlib import Path
from typing import Union, List, Optional
from ontology.ontology import (
    OntologyType,
    Strategy,
    ConversionResult,
    FailedId,
    OntologyBaseConverter,
    BaseOntologyFormatter,
)
from .types import SymptomOntologyFileFormat

# SYMP: Symptom Ontology ID, https://raw.githubusercontent.com/DiseaseOntology/SymptomOntology/v2022-11-30/src/ontology/symp.owl; https://bioportal.bioontology.org/ontologies/SYMP
# UMLS: Unified Medical Language System, https://www.nlm.nih.gov/research/umls/
# MESH: Medical Subject Headings, https://www.nlm.nih.gov/mesh/

DISEASE_DICT = OntologyType(
    type="Symptom", default="UMLS", choices=["UMLS", "MESH", "SYMP"]
)
