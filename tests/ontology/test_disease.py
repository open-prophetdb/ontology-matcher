# Unittest for disease.py

import unittest
from unittest.mock import patch, MagicMock
from ontology_matcher.disease import DiseaseOntologyFormatter, DiseaseOntologyConverter


# Test DiseaseOntologyConverter
class TestDiseaseOntologyConverter(unittest.TestCase):
    def setUp(self):
        ids = [
            "DOID:7402",
            "DOID:7400",
            "DOID:7401",
            "DOID:8731",
            "DOID:8729",
            "DOID:8725",
            "MESH:D015673",
        ]
        self.ids = ids
        self.disease_ontology_converter = DiseaseOntologyConverter(self.ids)

    def test_convert(self):
        self.disease_ontology_converter.convert()

        self.assertEqual(
            len(self.disease_ontology_converter.failed_ids),
            3,
            "Failed to convert disease ontology ids",
        )

        self.assertEqual(
            len(self.ids) - len(self.disease_ontology_converter.failed_ids), 4
        )

        print(
            self.disease_ontology_converter.failed_ids,
            self.disease_ontology_converter.converted_ids,
        )


# How to test the function in terminal?
# python -m unittest tests.ontology.test_disease.TestDiseaseOntologyConverter.test_convert
# python -m unittest tests.ontology.test_disease.TestDiseaseOntologyConverter.test_convert -v
