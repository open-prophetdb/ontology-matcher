import requests
from typing import List, Any
from dataclasses import dataclass
from tenacity import retry, stop_after_attempt, wait_random


@dataclass
class Entity:
    """Entity class."""

    synonyms: str
    description: str
    id: str
    name: str
    resource: str

    # Implement the __getitem__ method to access attributes as if it's a dictionary
    def __getitem__(self, key):
        return getattr(self, key)

    # Implement the __setitem__ method to set attributes as if it's a dictionary
    def __setitem__(self, key, value):
        setattr(self, key, value)

    def get(self, key, default=None):
        return getattr(self, key, default)


# Define a dataclass OLS4Doc which contains a list of attributes and can be used as a dict
@dataclass
class OLS4Doc:
    """OLS Doc class."""

    iri: str
    ontology_name: str
    synonym: List[str]
    short_form: str
    description: List[str]
    label: str
    obo_id: str
    type: str

    # Implement the __getitem__ method to access attributes as if it's a dictionary
    def __getitem__(self, key):
        return getattr(self, key)

    # Implement the __setitem__ method to set attributes as if it's a dictionary
    def __setitem__(self, key, value):
        setattr(self, key, value)

    def get(self, key, default: Any = None):
        return getattr(self, key, default)


class OLS4Query:
    """Query the OLS4 API.

    Args:
        - q: The query string, e.g. q=UBERON:0000948

        - exact: Whether to match the query exactly, the default is False

        - ontology: Restrict a search to a set of ontologies e.g. ontology=uberon,ma

        Such as:
        https://www.ebi.ac.uk/ols4/api/search?q=UBERON_0000948,UBERON_0001717&queryFields=short_form&ontology=uberon
        If we want to query the items exactly, we can set the exact parameter to True, queryFields to short_form, and convert the query string to UBERON_0000948,UBERON_0001717 with underline instead of colon. If we use colon, we may get several results.
    """

    # When you query the OLS4 API, you can specify the ontology parameter to restrict a search to a set of ontologies and it must be a lowercase string of the related key. The following is the supported ontologies.
    supported_ontologies = {
        "CHEBI": "Chemical Entities of Biological Interest",
        "CL": "Cell Ontology",
        "CLO": "Cell Line Ontology",
        "DOID": "Human Disease Ontology",
        "EFO": "Experimental Factor Ontology",
        "ExO": "Exposure Ontology",
        "GO": "Gene Ontology",
        "HP": "Human Phenotype Ontology",
        "MONDO": "Mondo Disease Ontology",
        "ORDO": "Orphanet Rare Disease Ontology",
        "PW": "Pathway Ontology",
        "SYMP": "Symptom Ontology",
        "UBERON": "Uber-anatomy ontology",
    }

    api_endpoint = "https://www.ebi.ac.uk/ols4/api/search"

    def __init__(
        self,
        q: List[str] | str,
        ontology: str,
        exact=True,
        **kwargs,
    ):
        # Check if the q has a correct prefix which must be one of the supported ontologies.
        if isinstance(q, str):
            q = [q]

        failed = []
        for q_item in q:
            if not any(
                q_item.startswith(ontology) for ontology in self.supported_ontologies
            ):
                failed.append(q_item)

        if len(failed) > 0:
            raise ValueError(
                f"The query string must have a correct prefix which must be one of the supported ontologies: {self.supported_ontologies.keys()}. The following query strings are not correct: {failed}"
            )

        self.q = list(map(lambda x: x.replace(":", "_"), q))

        if ontology not in self.supported_ontologies:
            raise ValueError(
                f"Ontology {ontology} is not supported currently. Please choose from {self.supported_ontologies.keys()}"
            )

        self.ontology = ontology.lower()
        self.exact = "true" if exact else "false"
        self.params = kwargs

        self.data = self._request()

    @retry(stop=stop_after_attempt(5), wait=wait_random(min=1, max=15))
    def _request(self) -> dict:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        }

        params = {
            "q": self.q,
            # Specifcy the fields to query, the defaults are {label, synonym, description, short_form, obo_id, annotations, logical_description, iri}, If we want to query the items exactly, you can set the queryFields to short_form. Don't change it.
            "queryFields": "short_form",
            "ontology": self.ontology,
            "exact": self.exact,
            **self.params,
        }
        response = requests.get(self.api_endpoint, headers=headers, params=params)
        return response.json()

    def parse(self) -> List[Entity]:
        """Parse the response data.

        Example:
        https://www.ebi.ac.uk/ols4/api/search?q=UBERON:0000948&queryFields=short_form&ontology=uberon&exact=true

        {
            "response": {
                "docs": [{
                    "iri": "http://purl.obolibrary.org/obo/UBERON_0000948",
                    "ontology_name": "uberon",
                    "synonym": [
                        "chambered heart",
                        "vertebrate heart",
                        "branchial heart",
                        "cardium"
                    ],
                    "short_form": "UBERON_0000948",
                    "description": [
                        "A myogenic muscular circulatory organ found in the vertebrate cardiovascular system composed of chambers of cardiac muscle. It is the primary circulatory organ.",
                        "Taxon notes:\" the ascidian tube-like heart lacks chambers....The ascidian heart is formed after metamorphosis as a simple tube-like structure with a single-layered myoepithelium that is continuous with a single-layered pericar- dial wall. It lacks chambers and endocardium.... The innovation of the chambered heart was a key event in vertebrate evolution, because the chambered heart generates one-way blood flow with high pressure, a critical requirement for the efficient blood supply of large-body vertebrates... all extant vertebrates have hearts with two or more chambers (Moorman and Christoffels 2003)\" DOI:10.1101/gad.1485706"
                    ],
                    "label": "heart",
                    "is_defining_ontology": true,
                    "obo_id": "UBERON:0000948",
                    "type": "class"
                }],
                "numFound": 1,
                "start": 0
            },
            "responseHeader": {
                "QTime": 26,
                "status": 0
            }
        }
        """
        # Please follow the above example to parse the response data. We need to get the synonym, description, obo_id, label fields.
        # The return value should be a dict.
        response = self.data.get("response", {})
        docs: List[OLS4Doc] = list(
            map(lambda doc: OLS4Doc(**doc), response.get("docs", []))
        )

        results: List[Entity] = []

        for item in self.q:
            raw_item = item.replace("_", ":")
            # Find the matched doc by the q value
            matched_docs: List[OLS4Doc] = list(
                filter(
                    lambda doc: doc.get("iri", "").split("/")[-1] == item
                    and doc.get("obo_id") == raw_item,
                    docs,
                )
            )

            if len(matched_docs) == 0:
                results.append(
                    Entity(
                        **{
                            "synonyms": "",
                            "description": "",
                            "id": raw_item,
                            "name": "",
                            "resource": raw_item.split(":")[0],
                        }
                    )
                )

            matched_doc = matched_docs[0]
            results.append(
                Entity(
                    **{
                        "synonyms": "|".join(matched_doc.get("synonym")),
                        "description": "\n".join(matched_doc.get("description")),
                        "id": raw_item,
                        "name": matched_doc.get("label"),
                        "resource": raw_item.split(":")[0],
                    }
                )
            )

        return results


class MyGene:
    """Query the MyGene API.

    Args:
        - q: The query string, e.g. q=1017,1018

        - scope: The scope of the query, e.g. scope=entrezgene

        - fields: Specify the fields to query, the defaults are {symbol, name, entrezgene, ensembl.gene, hgnc, alias, taxid, summary}
    """

    api_endpoint = "http://mygene.info/v3/query"
    fields = [
        "symbol",
        "name",
        "entrezgene",
        "ensembl",
        "HGNC",
        "MGI",
        "alias",
        "taxid",
        "summary",
    ]

    def __init__(self, q: str, scopes: str, fields: List[str] = [], **kwargs):
        self.q = q
        self.scopes = scopes
        self.fields = fields or self.fields
        self.params = kwargs

        self.data = self._request()

    @retry(stop=stop_after_attempt(5), wait=wait_random(min=1, max=15))
    def _request(self) -> List[dict]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        }

        payload = {
            "q": self.q,
            "fields": ",".join(self.fields),
            "scopes": self.scopes,
            **self.params,
        }

        response = requests.post(self.api_endpoint, headers=headers, json=payload)
        return response.json()

    def parse(self) -> List[dict]:
        """Parse the response data.

        Example:
        [
            {
                'query': '1017',
                '_id': '1017',
                '_score': 22.757837,
                'entrezgene': 1017,
                'name': 'cyclin dependent kinase 2',
                'query': '1017',
                'symbol': 'CDK2',
                'taxid': 9606
            },
            {
                'query': '1018',
                '_id': '1018',
                '_score': 22.757782,
                'entrezgene': 1018,
                'name': 'cyclin dependent kinase 3',
                'query': '1018',
                'symbol': 'CDK3',
                'taxid': 9606
            }
        ]
        """
        return self.data
