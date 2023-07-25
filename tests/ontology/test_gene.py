# Unittest for gene.py

import unittest
from unittest.mock import patch, MagicMock
from ontology_matcher.gene import GeneOntologyConverter


# Test GeneOntologyConverter
class TestGeneOntologyConverter(unittest.TestCase):
    def setUp(self):
        ids = [
            "ENTREZ:27777",
            "MGI:1342288",
            "HGNC:52949",
            "ENSEMBL:ENSG00000238211",
            "SYMBOL:TP53", # Multiple mappings between SYMBOL and ENTREZ
            "SYMBOL:PNPT1P2", # Multiple mappings between SYMBOL and ENTREZ
            "SYMBOL:NOTFOUND", # not found
            "HGNC:NOTFOUND", # not found
        ]
        self.ids = ids
        self.gene_ontology_converter = GeneOntologyConverter(self.ids)

    def test_convert(self):
        self.gene_ontology_converter.convert()

        print(self.gene_ontology_converter.converted_ids)
        print(self.gene_ontology_converter.failed_ids)

        self.assertEqual(
            len(self.gene_ontology_converter.failed_ids),
            4,
            "Failed to convert gene ontology ids",
        )

        self.assertEqual(
            len(self.ids) - len(self.gene_ontology_converter.failed_ids), 4
        )


# How to test the function in terminal?
# python -m unittest tests.ontology.test_gene.TestGeneOntologyConverter.test_convert
# python -m unittest tests.ontology.test_gene.TestGeneOntologyConverter.test_convert -v
