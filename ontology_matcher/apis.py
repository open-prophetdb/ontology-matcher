import requests
import logging
from typing import List, Any, Dict
from dataclasses import dataclass
from enum import Enum
from tenacity import retry, stop_after_attempt, wait_random
from ontology_matcher.ontology_formatter import (
    ConvertedId,
    make_grouped_ids,
    flatten_dedup,
)

logger = logging.getLogger("ontology_matcher.apis")


@dataclass
class Entity:
    """Entity class."""

    synonyms: List[str]
    xrefs: List[str]
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
    """Query the OLS4 API. We can use this API to update information for the disease entity. So we don't need the users to provide a full disease ontology file, they can just provide a set of ids.

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
        q: str,
        ontology: str,
        exact=True,
        **kwargs,
    ):
        # Check if the q has a correct prefix which must be one of the supported ontologies.
        if not isinstance(q, str):
            raise ValueError("The query string must be a string.")

        self.q = q.replace(":", "_").split(",")

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

        logger.debug("Params: %s" % params)
        response = requests.get(self.api_endpoint, headers=headers, params=params)
        return response.json()

    def parse(self) -> List[Entity]:
        """Parse the response data.

        Example:
        https://www.ebi.ac.uk/ols4/api/search?q=UBERON_0000948&queryFields=short_form&ontology=uberon&exact=true

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
                            "synonyms": [],
                            "description": "",
                            "id": raw_item,
                            "name": "",
                            "resource": raw_item.split(":")[0],
                            "xrefs": [],
                        }
                    )
                )
            else:
                matched_doc = matched_docs[0]
                results.append(
                    Entity(
                        **{
                            "synonyms": matched_doc.get("synonym"),
                            "description": "\n".join(matched_doc.get("description")),
                            "id": raw_item,
                            "name": matched_doc.get("label"),
                            "resource": raw_item.split(":")[0],
                            "xrefs": [],
                        }
                    )
                )

        return results

    @classmethod
    def update_metadata(
        cls, converted_ids: List[ConvertedId], database: str
    ) -> List[ConvertedId]:
        logger.debug("OLS4Query.update_metadata...")

        def get_id(x: str | List[str]) -> str | None:
            if isinstance(x, list) and len(x) == 1:
                return x[0]
            elif isinstance(x, str):
                return x
            else:
                return None

        selected_id_pair: Dict[str, str] = {
            # Stupid warning: get_id(x.get(database)) must be not None, because if clause has already checked it.
            # type: ignore
            x.get_raw_id(): [idx, get_id(x.get(database))]
            for idx, x in enumerate(converted_ids)
            if get_id(x.get(database))
        }
        idx_ids = list(selected_id_pair.values())
        logger.debug("converted_ids: %s" % converted_ids)
        logger.debug("Selected id pair: %s" % selected_id_pair)
        logger.debug("Ids: %s" % idx_ids)

        ids = [x[1] for x in idx_ids]
        index_id_dict = {x[1]: x[0] for x in idx_ids}
        grouped_ids = make_grouped_ids(ids)
        logger.debug("Grouped ids: %s" % grouped_ids)
        logger.debug("Index-id dict: %s" % index_id_dict)

        id_dict = grouped_ids.id_dict

        # Groups may be similar to 'SYMP', 'MESH', etc.
        groups = id_dict.keys()
        logger.debug("Groups: %s" % groups)

        valid_keys = set(groups).intersection(set(cls.supported_ontologies.keys()))
        logger.debug("Valid keys: %s" % valid_keys)

        for group in valid_keys:
            ids = [f"{group}:{x}" for x in id_dict.get(group, [])]
            ids = ",".join(ids)

            query = cls(q=ids, ontology=group, exact=True)
            results = query.parse()

            for index, result in enumerate(results):
                id = result.id
                idx = int(index_id_dict.get(id, "-1"))
                if idx != -1:
                    matched = converted_ids[idx]

                    logger.debug(
                        "Matched ConvertedId: %s, %s" % (matched, result.__dict__),
                    )

                    matched.update_metadata(result.__dict__)
                else:
                    logger.warning("Cannot find the id %s in the converted ids." % id)

        logger.debug("MyDisease.update_metadata done.\n\n")
        return converted_ids


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
        # For more details on the mygene API, please refer to https://docs.mygene.info/en/latest/doc/query_service.html
        if "MGI" in scopes.split(","):
            # The MGI id should be MGI:MGI:1342288, so we need to add the prefix.
            ids = [f"MGI:{x}" for x in q.split(",")]
            q = ",".join(ids)

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
                'taxid': '9606'
            },
            {
                'query': '1018',
                '_id': '1018',
                '_score': 22.757782,
                'entrezgene': 1018,
                'name': 'cyclin dependent kinase 3',
                'query': '1018',
                'symbol': 'CDK3',
                'taxid': '9606'
            }
        ]
        """
        return self.data


class EntityType(Enum):
    """The entity type."""

    COMPOUND = "compound"
    METABOLITE = "metabolite"


class MyChemical:
    """A API wrapper for mychem.info. It can deal with common chemical ids. Such as pubchem, chebi, drugbank,  mesh, umls, pharmgkb, etc. So we can use it to convert the ids for metabolites, compounds etc. The http://cts.fiehnlab.ucdavis.edu/ is also a good choice, but it doesn't update frequently."""

    # More details on fields: https://docs.mychem.info/en/latest/doc/data.html#available-fields
    DEFAULT_FEILDS = [
        "chebi.xrefs.drugbank",
        "chebi.xrefs.hmdb",
        "chebi.xrefs.pubchem.cid",
        "chebi.id",
        "chebi.name",
        "chebi.definition",
        "chebi.synonyms",
        "chebi.citation.pubmed",
        "pubchem.cid",
        "drugbank.name",
        "drugbank.synonyms",
        "drugbank.id",
        "umls.name",
        "umls.cui",
        "umls.mesh",
        "pharmgkb.name",
        "pharmgkb.trade_names",
        "pharmgkb.generic_names",
        "pharmgkb.xrefs.chebi",
        "pharmgkb.xrefs.drugbank",
        "pharmgkb.xrefs.pubchem.cid",
        "pharmgkb.xrefs.hmdb",
        "pharmgkb.xrefs.mesh",
        "pharmgkb.xrefs.umls",
        "chembl.pref_name",
        "chembl.molecule_chembl_id",
        "chembl.molecule_synonyms",
        "ginas.preferred_name",
        "ginas.xrefs.MESH",
    ]

    SUPPORTED_SCOPES = {
        "PUBCHEM": "pubchem.cid,pharmgkb.xrefs.pubchem.cid",
        "CHEBI": "chebi.id,pharmgkb.xrefs.chebi",
        "MESH": "umls.mesh,pharmgkb.xrefs.mesh,ginas.xrefs.MESH,chembl.drug_indications.mesh_id",
        "DrugBank": "drugbank.id,pharmgkb.xrefs.drugbank",
        "UMLS": "umls.cui,pharmgkb.xrefs.umls",
        "HMDB": "unichem.hmdb,pharmgkb.xrefs.hmdb,chebi.xrefs.hmdb",
        "CHEMBL": "chembl.molecule_chembl_id",
    }

    api_endpoint = "http://mychem.info/v1/query"
    # Example: https://docs.mychem.info/en/latest/doc/chem_query_service.html#id6

    def __init__(
        self,
        q: List[str],
        entity_type: EntityType = EntityType.COMPOUND,
        **kwargs,
    ):
        prefixes = [x.split(":")[0] for x in q]
        if len(set(prefixes)) > 1:
            raise ValueError("The query strings must have the same prefix.")
        else:
            prefix = prefixes[0]
            if prefix not in self.SUPPORTED_SCOPES:
                raise ValueError(
                    f"Prefix {prefix} is not supported currently. Please choose from {self.SUPPORTED_SCOPES.keys()}"
                )

        self.q = q
        self.database = prefix
        self.entity_type = (
            "Compound" if entity_type == EntityType.COMPOUND else "Metabolite"
        )
        self.scopes = self.SUPPORTED_SCOPES.get(prefix)
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
            "q": [x.split(":")[1] for x in self.q]
            if self.database != "CHEBI"
            else self.q,
            "fields": ",".join(self.DEFAULT_FEILDS),
            "scopes": self.scopes,
            **self.params,
        }

        # logger.debug("Payload: %s" % payload)
        response = requests.post(self.api_endpoint, headers=headers, json=payload)
        return response.json()

    def _convert2list(
        self, x: List[List[str] | str] | List[str] | str, database: str
    ) -> List[str]:
        def _add_prefix(x: str, database: str) -> str:
            # The CHEBI id has a prefix CHEBI, but the other ids don't have a prefix.
            if database == "CHEBI":
                return x
            else:
                return f"{database}:{x}"

        if isinstance(x, list):
            x = flatten_dedup(x)
            return list(map(lambda y: _add_prefix(y, database), x))
        elif isinstance(x, str):
            return [_add_prefix(x, database)]
        else:
            return []

    def _get_chebi(self, chebi_data: dict | List[Dict]) -> dict:
        """Get the chebi id from the data."""
        if isinstance(chebi_data, dict):
            xrefs = chebi_data.get("xrefs", {})
            CHEMBL = self._convert2list(xrefs.get("chembl", []), "CHEMBL")
            DRUGBANK = self._convert2list(xrefs.get("drugbank", []), "DrugBank")
            HMDB = self._convert2list(xrefs.get("hmdb", []), "HMDB")
            PUBCHEM = self._convert2list(
                xrefs.get("pubchem", {}).get("cid", []), "PUBCHEM"
            )
            CHEBI = self._convert2list(chebi_data.get("id", ""), "CHEBI")

            return {
                "metadata": {
                    "name": chebi_data.get("name", ""),
                    "description": chebi_data.get("definition", ""),
                    "synonyms": chebi_data.get("synonyms", []),
                    "pmids": chebi_data.get("citation", {}).get("pubmed", []),
                },
                "CHEBI": CHEBI,
                "CHEMBL": CHEMBL,
                "DrugBank": DRUGBANK,
                "HMDB": HMDB,
                "PUBCHEM": PUBCHEM,
            }
        elif isinstance(chebi_data, list):
            CHEBIS = []
            DRUGBANKS = []
            PUBCHEMS = []
            CHEMBLS = []
            HMDBS = []
            for item in chebi_data:
                xrefs = item.get("xrefs", {})
                CHEBIS.extend(self._convert2list(item.get("id", ""), "CHEBI"))
                DRUGBANKS.extend(
                    self._convert2list(xrefs.get("drugbank", []), "DrugBank")
                )
                PUBCHEMS.extend(
                    self._convert2list(
                        xrefs.get("pubchem", {}).get("cid", []), "PUBCHEM"
                    )
                )
                CHEMBLS.extend(self._convert2list(xrefs.get("chembl", []), "CHEMBL"))
                HMDBS.extend(self._convert2list(xrefs.get("hmdb", []), "HMDB"))

            return {
                "metadata": {},
                "CHEBI": CHEBIS,
                "CHEMBL": CHEMBLS,
                "DrugBank": DRUGBANKS,
                "HMDB": HMDBS,
                "PUBCHEM": PUBCHEMS,
            }

        else:
            return {}

    def _get_pubchem(self, pubchem_data: dict | List[Dict]) -> dict:
        """Get the pubchem id from the data."""
        if isinstance(pubchem_data, dict):
            return {
                "metadata": {},
                "PUBCHEM": self._convert2list(pubchem_data.get("cid", {}), "PUBCHEM"),
            }
        elif isinstance(pubchem_data, list):
            cids = []
            for item in pubchem_data:
                cids.extend(self._convert2list(item.get("cid", {}), "PUBCHEM"))

            return {
                "metadata": {},
                "PUBCHEM": cids,
            }
        else:
            return {}

    def _get_drugbank(self, drugbank_data: dict | List[Dict]) -> dict:
        """Get the drugbank id from the data."""
        if isinstance(drugbank_data, dict):
            return {
                "metadata": {
                    "name": drugbank_data.get("name", ""),
                    "synonyms": drugbank_data.get("synonyms", []),
                },
                "DrugBank": self._convert2list(drugbank_data.get("id", {}), "DrugBank"),
            }
        elif isinstance(drugbank_data, list):
            drugbank_ids = []
            for item in drugbank_data:
                drugbank_ids.extend(self._convert2list(item.get("id", {}), "DrugBank"))

            return {
                "metadata": {},
                "DrugBank": drugbank_ids,
            }
        else:
            return {}

    def _get_umls(self, umls_data: dict | List[Dict]) -> dict:
        """Get the umls id from the data."""
        if isinstance(umls_data, dict):
            return {
                "metadata": {
                    "name": umls_data.get("name", ""),
                },
                "UMLS": self._convert2list(umls_data.get("cui", {}), "UMLS"),
                "MESH": self._convert2list(umls_data.get("mesh", {}), "MESH"),
            }
        elif isinstance(umls_data, list):
            umls_ids = []
            mesh_ids = []
            for item in umls_data:
                umls_ids.extend(self._convert2list(item.get("cui", {}), "UMLS"))
                mesh_ids.extend(self._convert2list(item.get("mesh", {}), "MESH"))

            return {
                "metadata": {},
                "UMLS": umls_ids,
                "MESH": mesh_ids,
            }
        else:
            return {}

    def concat(self, x: str | List[str], y: str | List[str]) -> List[str]:
        if isinstance(x, str):
            x = [x]
        if isinstance(y, str):
            y = [y]

        return list(set(x + y))

    def _get_hmdb(self, pharmgkb_data: dict | List[Dict]) -> dict:
        """Get the hmdb id from the data. NOTICE: we cannot get hmdb data from the mychem.info directly. But we may can get the hmdb id from the pharmgkb data"""
        if isinstance(pharmgkb_data, dict):
            return {
                "metadata": {
                    "name": pharmgkb_data.get("name", ""),
                    "synonyms": self.concat(
                        pharmgkb_data.get("trade_names", []),
                        pharmgkb_data.get("generic_names", []),
                    ),
                },
                "HMDB": self._convert2list(
                    pharmgkb_data.get("xrefs", {}).get("hmdb", []), "HMDB"
                ),
            }
        elif isinstance(pharmgkb_data, list):
            hmdb_ids = []
            for item in pharmgkb_data:
                hmdb_ids.extend(
                    self._convert2list(item.get("xrefs", {}).get("hmdb", []), "HMDB")
                )

            return {
                "metadata": {},
                "HMDB": hmdb_ids,
            }
        else:
            return {}

    def _get_chembl(self, chembl_data: dict | List[Dict]) -> dict:
        """Get the chembl id from the data."""
        if isinstance(chembl_data, dict):
            molecule_synonyms = chembl_data.get("molecule_synonyms", [])
            if isinstance(molecule_synonyms, dict):
                synonyms = flatten_dedup(
                    [molecule_synonyms.get("molecule_synonym", [])]
                )
            elif isinstance(molecule_synonyms, list):
                synonyms = list(
                    map(
                        lambda x: x.get("molecule_synonym"),
                        chembl_data.get("molecule_synonyms", {}),
                    )
                )
            else:
                synonyms = []

            return {
                "metadata": {
                    "name": chembl_data.get("pref_name", ""),
                    "synonyms": synonyms,
                },
                "CHEMBL": self._convert2list(
                    chembl_data.get("molecule_chembl_id", []), "CHEMBL"
                ),
            }
        elif isinstance(chembl_data, list):
            chembl_ids = []
            for item in chembl_data:
                chembl_ids.extend(
                    self._convert2list(item.get("molecule_chembl_id", []), "CHEMBL")
                )

            return {
                "metadata": {},
                "CHEMBL": chembl_ids,
            }
        else:
            return {}

    def _get_mesh(self, ginas_data: dict | List[Dict]) -> dict:
        """Get the mesh id from the data."""
        if isinstance(ginas_data, dict):
            return {
                "metadata": {
                    "name": ginas_data.get("preferred_name", ""),
                },
                "MESH": self._convert2list(
                    ginas_data.get("xrefs", {}).get("MESH", []), "MESH"
                ),
            }
        elif isinstance(ginas_data, list):
            mesh_ids = []
            for item in ginas_data:
                mesh_ids.extend(
                    self._convert2list(item.get("xrefs", {}).get("MESH", []), "MESH")
                )

            return {
                "metadata": {},
                "MESH": mesh_ids,
            }
        else:
            return {}

    def _update_dict(self, x: dict, y: dict) -> dict:
        """Update the dict x with the dict y. It follows the rules:
        1. If the key exists either in x or y, we will use the existing value.
        2. If the key exists both in x and y, we will merge the values. If the value is a list, we will merge the lists. If the value is a str or int, we will the y value (because we have make all the values to be a list if it can have multiple values). If the value is a dict, we will merge the dicts and follow the rule 1 and 2.
        """
        for key, value in y.items():
            if key in x:
                if isinstance(value, list):
                    x_value = x.get(key, [])
                    x[key] = self.concat(x_value, value)
                elif isinstance(value, dict):
                    x_value = x.get(key, {})
                    if not isinstance(x_value, dict):
                        x_value = {}
                    x[key] = self._update_dict(x_value, value)
                else:
                    x[key] = value
            else:
                x[key] = value

        return x

    def parse(self) -> List[Dict[str, Any]]:
        """Parse the response data."""

        def get_xrefs(result: dict) -> List[str]:
            """Get the xrefs from the result."""
            xrefs = []
            for key, value in result.items():
                if key in [
                    "CHEBI",
                    "CHEMBL",
                    "DrugBank",
                    "HMDB",
                    "PUBCHEM",
                    "UMLS",
                    "MESH",
                ]:
                    xrefs.extend(value)

            return list(set(xrefs))

        results: List[Dict[str, Any]] = []
        for index, item in enumerate(self.q):
            # Find the matched doc by the q value
            matched_docs = list(
                filter(
                    lambda doc: doc.get("query") == item.split(":")[1]
                    if self.database != "CHEBI"
                    else doc.get("query") == item,
                    self.data,
                )
            )

            if len(matched_docs) == 0:
                results.append(
                    {
                        "idx": index,
                        "raw_id": item,
                        "resource": item.split(":")[0],
                        "label": self.entity_type,
                        self.database: item,
                        "metadata": {
                            "synonyms": "",
                            "description": "",
                            "name": "",
                            "resource": item.split(":")[0],
                            "xrefs": [],
                        },
                    }
                )
            else:
                result: Dict[str, Any] = {
                    "metadata": {},
                }

                for doc in matched_docs:
                    result = self._update_dict(
                        result, self._get_chebi(doc.get("chebi", {}))
                    )
                    result = self._update_dict(
                        result, self._get_pubchem(doc.get("pubchem", {}))
                    )
                    result = self._update_dict(
                        result, self._get_drugbank(doc.get("drugbank", {}))
                    )
                    result = self._update_dict(
                        result, self._get_umls(doc.get("umls", {}))
                    )
                    result = self._update_dict(
                        result, self._get_hmdb(doc.get("pharmgkb", {}))
                    )
                    result = self._update_dict(
                        result, self._get_chembl(doc.get("chembl", {}))
                    )
                    result = self._update_dict(
                        result, self._get_mesh(doc.get("ginas", {}))
                    )

                result.update(
                    {
                        "idx": index,
                        "raw_id": item,
                        "resource": item.split(":")[0],
                        "label": self.entity_type,
                    }
                )
                result.get("metadata", {}).update({"xrefs": get_xrefs(result)})
                results.append(result)

                logger.debug("Result: %s" % result)

        return results


class MyDisease:
    """A API wrapper for mydisease.info. It can deal with common disease ids. Such as DOID, MESH, OMIM, ORDO, etc. So we can use it to convert the ids for diseases."""

    FIELDS = {"update_metadata": ["mondo", "disease_ontology", "umls"]}

    SUPPORTED_SCOPES = {
        "MONDO": "_id",
        "DOID": "disease_ontology.doid",
    }

    api_endpoint = "https://mydisease.info/v1/query"
    # Example: https://mydisease.info/v1/query?fields=mondo&size=10&from=0&fetch_all=false

    def __init__(
        self,
        q: List[str],
        purpose: str = "update_metadata",
        **kwargs,
    ):
        prefixes = [x.split(":")[0] for x in q]
        if len(set(prefixes)) > 1:
            raise ValueError("The query strings must have the same prefix.")
        else:
            prefix = prefixes[0]
            if prefix not in self.SUPPORTED_SCOPES:
                raise ValueError(
                    f"Prefix {prefix} is not supported currently. Please choose from {self.SUPPORTED_SCOPES.keys()}"
                )

        self.q = q
        self.scopes = self.SUPPORTED_SCOPES.get(prefix)
        self.fields = self.FIELDS.get(purpose, [])
        self.fetch_all = "false"
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
            "fetch_all": self.fetch_all,
            **self.params,
        }

        # logger.debug("Payload: %s" % payload)
        response = requests.post(self.api_endpoint, headers=headers, json=payload)
        return response.json()

    def format_xrefs(self, xrefs: dict) -> List[str]:
        doid = flatten_dedup(xrefs.get("doid", []))
        mesh = list(
            map(lambda x: "MESH:%s" % x, flatten_dedup([xrefs.get("mesh", [])]))
        )

        orphanet = list(
            map(lambda x: "ORDO:%s" % x, flatten_dedup(xrefs.get("orphanet", [])))
        )
        ordo = list(map(lambda x: "ORDO:%s" % x, flatten_dedup(xrefs.get("ordo", []))))
        ordo = list(set(orphanet + ordo))

        umls = list(map(lambda x: "UMLS:%s" % x, flatten_dedup(xrefs.get("umls", []))))
        umls_cui = list(
            map(lambda x: "UMLS:%s" % x, flatten_dedup(xrefs.get("umls_cui", [])))
        )
        umls = list(set(umls + umls_cui))

        icd9 = list(map(lambda x: "ICD-9:%s" % x, flatten_dedup(xrefs.get("icd9", []))))
        icd9cm = list(
            map(lambda x: "ICD-9:%s" % x, flatten_dedup(xrefs.get("icd9cm", [])))
        )
        icd9 = list(set(icd9 + icd9cm))

        icd10 = list(
            map(lambda x: "ICD10CM:%s" % x, flatten_dedup(xrefs.get("icd10", [])))
        )
        icd10cm = list(
            map(lambda x: "ICD10CM:%s" % x, flatten_dedup(xrefs.get("icd10cm", [])))
        )
        icd10 = list(set(icd10 + icd10cm))

        hp = list(map(lambda x: "HP:%s" % x, flatten_dedup(xrefs.get("hp", []))))
        omim = list(map(lambda x: "OMIM:%s" % x, flatten_dedup(xrefs.get("omim", []))))

        return list(set(doid + mesh + orphanet + umls + icd9 + icd10 + hp + omim))

    def parse(self) -> List[Entity]:
        """Parse the response data.

        Example:
        http://mydisease.info/v1/disease/MONDO:0016575
        """
        results: List[Entity] = []
        for item in self.q:
            # Find the matched doc by the q value
            matched_docs = list(
                filter(
                    lambda doc: doc.get("query") == item,
                    self.data,
                )
            )

            if len(matched_docs) == 0:
                results.append(
                    Entity(
                        **{
                            "synonyms": [],
                            "description": "",
                            "id": item,
                            "name": "",
                            "resource": item.split(":")[0],
                            "xrefs": [],
                        }
                    )
                )
            else:
                matched_doc = matched_docs[0]
                mondo = matched_doc.get("mondo")
                do = matched_doc.get("disease_ontology")
                name = ""
                synonyms = []
                xrefs = []
                description = ""

                if mondo:
                    name = mondo.get("label")
                    synonyms = mondo.get("synonym", {}).get("exact", [])
                    description = mondo.get("definition", "")
                    xrefs = self.format_xrefs(mondo.get("xrefs", {}))
                elif do:
                    name = do.get("name")
                    synonyms = do.get("synonyms", {}).get("exact", [])
                    description = do.get("def", "")
                    xrefs = self.format_xrefs(do.get("xrefs", {}))

                results.append(
                    Entity(
                        **{
                            "synonyms": synonyms
                            if isinstance(synonyms, list)
                            else [synonyms],
                            "description": description,
                            "id": item,
                            "name": name,
                            "resource": item.split(":")[0],
                            "xrefs": xrefs,
                        }
                    )
                )

        return results

    @classmethod
    def update_metadata(
        cls, converted_ids: List[ConvertedId], database: str
    ) -> List[ConvertedId]:
        logger.debug("MyDisease.update_metadata...")
        def get_id(x: str | List[str]) -> str | None:
            if isinstance(x, list) and len(x) == 1:
                return x[0]
            elif isinstance(x, str):
                return x
            else:
                return None

        selected_id_pair: Dict[str, str] = {
            # Stupid warning: get_id(x.get(database)) must be not None, because if clause has already checked it.
            # type: ignore
            x.get_raw_id(): [idx, get_id(x.get(database))]
            for idx, x in enumerate(converted_ids)
            if get_id(x.get(database))
        }
        idx_ids = list(selected_id_pair.values())
        logger.debug("converted_ids: %s" % converted_ids)
        logger.debug("Selected id pair: %s" % selected_id_pair)
        logger.debug("Ids: %s" % idx_ids)

        ids = [x[1] for x in idx_ids]
        index_id_dict = {x[1]:x[0] for x in idx_ids}
        grouped_ids = make_grouped_ids(ids)
        logger.debug("Grouped ids: %s" % grouped_ids)
        logger.debug("Index-id dict: %s" % index_id_dict)

        id_dict = grouped_ids.id_dict

        # Groups may be similar to 'DOID', 'MONDO', etc.
        groups = id_dict.keys()
        logger.debug("Groups: %s" % groups)

        valid_keys = set(groups).intersection(set(cls.SUPPORTED_SCOPES.keys()))
        logger.debug("Valid keys: %s" % valid_keys)

        for group in valid_keys:
            ids = [f"{group}:{x}" for x in id_dict.get(group, [])]

            query = cls(ids, ontology=group, exact=True)
            results = query.parse()

            for index, result in enumerate(results):
                id = result.id
                idx = int(index_id_dict.get(id, '-1'))
                if idx != -1:
                    matched = converted_ids[idx]

                    logger.debug(
                        "Matched ConvertedId: %s, %s"
                        % (
                            matched,
                            result.__dict__                        
                        ),
                    )

                    matched.update_metadata(result.__dict__)
                else:
                    logger.warning("Cannot find the id %s in the converted ids." % id)

        logger.debug("MyDisease.update_metadata done.\n\n")
        return converted_ids


class MyVariant:
    """A API wrapper for myvariant.info. It can deal with common variant ids. Such as dbSNP, HGVS, ClinVar, etc. So we can use it to convert the ids for variants."""

    pass
