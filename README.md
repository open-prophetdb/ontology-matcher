## Ontology Matcher

It's a simple ontology matcher for building a set of cleaned ontologies. These ontologies will be used for building a knowledge graph.

When we want to build a knowledge graph, we always have a problem that the ids of entities are not consistent. The same entity may have different ids in different databases. For example, the entity "Fatigue Syndrom, Chronic" has the following ids in different databases: [MESH:D015673](https://meshb.nlm.nih.gov/record/ui?ui=D015673) and [DOID:8544](https://disease-ontology.org/term/DOID:8544/). But they are the same entity, if we want to integrate the knowledges from different databases which may use different ids to describe the same entity, we need to match the ids of the same entity. This is the purpose of this project.

It is the fundamental step for building a knowledge graph. The knowledge graph is the basis of many applications, such as question answering, knowledge base completion, etc.

NOTICE: It's not production ready, we will continue to improve it.

### Sister Projects

- [Knowledge Graph Studio](https://github.com/yjcyxky/biomedgps-studio): A web application for visualizing, editing and discovering knowledge graphs.

- [Knowledge Graph Backend](https://github.com/yjcyxky/rapex): A backend for storing and querying knowledge graph which is built for the Rapex project.

### How to use

#### 1. Install

```bash
git clone https://github.com/yjcyxky/ontology-matcher.git

cd ontology-matcher

python setup.py install
```

#### 2. Examples

Output the help information:

```bash
onto-match --help

# Usage: onto-match [OPTIONS] COMMAND [ARGS]...

# Options:
#   --help  Show this message and exit.

# Commands:
#   convert   Convert ontology ids.
#   idtypes   Which ID types are supported.
#   template  Generate input file template
```

Output the supported id types:

```bash
onto-match idtypes --help

# Usage: onto-match idtypes [OPTIONS]

#   Which ID types are supported.

# Options:
#   -O, --ontology-type [disease|gene]
#                                   Ontology type  [required]
#   --help                          Show this message and exit.

onto-match idtypes -O disease

# Outpus as follows:
# OMIM
# DOID
```

Output the input file template:

```bash
onto-match template --help

# Usage: onto-match template [OPTIONS]

#   Generate input file template

# Options:
#   -O, --ontology-type [disease|gene]
#                                   Ontology type  [required]
#   -o, --output-file TEXT          Path to output file  [required]
#   --help                          Show this message and exit.

onto-match template -O disease -o input.tsv

# Output as follows:
# ID	name	:LABEL	resource
# DOID:4001	ovarian carcinoma	Disease	DOID
# MESH:D015673	Fatigue Syndrom, Chronic	Disease	DOID
```

### Current Status

|     Ontology Type      |        Database         | Number of Entities |      Plan       |
| :--------------------: | :---------------------: | :----------------: | :-------------: |
|        Disease         |     DOID;MESH;OMIM      |       13,270       |        √        |
|          Gene          |         ENTREZ          |      589,823       |        √        |
| Compound/Chemical/Drug |      DRUGBANK;MESH      |      175,910       |        √        |
|        Pathway         | KEGG;WIKIPATHWAYS;REACT |        2567        |        √        |
|   CellularComponent    |           GO            |        1132        |        √        |
|   MolecularFunction    |           GO            |        2068        |        √        |
|   BiologicalProcess    |           GO            |        9072        |        √        |
|        Anatomy         |       UBERON;MESH       |        1844        |        √        |
|   PharmacologicClass   |     HETIONET;NDF-RT     |       45,605       |        √        |
|        Protein         |                         |                    |  Copy from CKG  |
|       SideEffect       |                         |                    | To be continued |
|       Metabolite       |                         |                    | To be continued |
|        Symptom         |                         |                    | To be continued |
