import requests
from typing import List
from tenacity import retry, stop_after_attempt, wait_random


class OLS4Query:
    """Query the OLS4 API.

    Args:
        - q: The query string, e.g. q=UBERON:0000948

        - queryFields: Specifcy the fields to query, the defaults are {label, synonym, description, short_form, obo_id, annotations, logical_description, iri}

        - exact: Whether to match the query exactly, the default is False

        - ontology: Restrict a search to a set of ontologies e.g. ontology=uberon,ma
    """

    api_endpoint = "https://www.ebi.ac.uk/ols4/api/search"

    def __init__(self, q, ontology, queryFields="obo_id", exact=False, **kwargs):
        self.q = q
        self.queryFields = queryFields
        self.ontology = ontology
        self.exact = exact
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
            "queryFields": self.queryFields,
            "ontology": self.ontology,
            "exact": self.exact,
            **self.params,
        }
        response = requests.get(self.api_endpoint, headers=headers, params=params)
        return response.json()

    def parse(self) -> dict:
        """Parse the response data.

        Example:
        https://www.ebi.ac.uk/ols4/api/search?q=UBERON:0000948&queryFields=obo_id&ontology=uberon&exact=true

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
                    "short_form": [
                        "UBERON_0000948"
                    ],
                    "description": [
                        "A myogenic muscular circulatory organ found in the vertebrate cardiovascular system composed of chambers of cardiac muscle. It is the primary circulatory organ.",
                        "Taxon notes:\" the ascidian tube-like heart lacks chambers....The ascidian heart is formed after metamorphosis as a simple tube-like structure with a single-layered myoepithelium that is continuous with a single-layered pericar- dial wall. It lacks chambers and endocardium.... The innovation of the chambered heart was a key event in vertebrate evolution, because the chambered heart generates one-way blood flow with high pressure, a critical requirement for the efficient blood supply of large-body vertebrates... all extant vertebrates have hearts with two or more chambers (Moorman and Christoffels 2003)\" DOI:10.1101/gad.1485706"
                    ],
                    "label": [
                        "heart"
                    ],
                    "is_defining_ontology": true,
                    "obo_id": [
                        "UBERON:0000948"
                    ],
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
        response = self.data.get("response")
        docs = response.get("docs")

        # Find the matched doc by the q value
        matched_docs = filter(
            lambda doc: doc.get("iri").split("/")[-1] == self.q.replace(":", "_"), docs
        )

        if len(matched_docs) == 0:
            return None

        matched_doc = matched_docs[0]
        return {
            "synonyms": ",".join(matched_doc.get("synonym")),
            "description": ". ".join(matched_doc.get("description")),
            "id": self.q,
            "name": matched_doc.get("label")[0],
        }


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
        "MGI"
        "alias",
        "taxid",
        "summary",
    ]

    def __init__(self, q: str, scopes: str, fields: List[str] = None, **kwargs):
        self.q = q
        self.scopes = scopes
        self.fields = fields or self.fields
        self.params = kwargs

        self.data = self._request()

    @retry(stop=stop_after_attempt(5), wait=wait_random(min=1, max=15))
    def _request(self) -> dict:
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
