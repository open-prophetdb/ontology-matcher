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

pip install -r requirements.txt
```