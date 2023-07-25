# Unittest for disease.py

import unittest
import logging
from unittest.mock import patch, MagicMock
from ontology_matcher.disease import DiseaseOntologyFormatter, DiseaseOntologyConverter

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Test DiseaseOntologyConverter
class TestDiseaseOntologyConverter(unittest.TestCase):
    def setUp(self):
        ids = [
            "DOID:7402",
            "MESH:D015673",
            "HP:0030358",
            "ORDO:94063",
            "UMLS:C0007131",
            "ICD-9:349.89", # Multiple mappings between ICD-9 and MONDO
            "ICD10CM:C80", # Cannot find a mapping between ICD10CM and MONDO
            "DOID:notexist", # not exist
            "MESH:notexist", # not exist
        ]
        self.ids = ids
        self.disease_ontology_converter = DiseaseOntologyConverter(self.ids)

    def test_convert(self):
        self.disease_ontology_converter.convert()

        print(self.disease_ontology_converter.converted_ids)
        print(self.disease_ontology_converter.failed_ids)

        self.assertEqual(
            len(self.disease_ontology_converter.failed_ids),
            4,
            "Failed to convert disease ontology ids",
        )

        self.assertEqual(
            len(self.ids) - len(self.disease_ontology_converter.failed_ids), 5
        )

# How to test the function in terminal?
# python -m unittest tests.ontology.test_disease.TestDiseaseOntologyConverter.test_convert
# python -m unittest tests.ontology.test_disease.TestDiseaseOntologyConverter.test_convert -v
