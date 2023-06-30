The platform supports the following ontologies. Each ontology may have several identifiers and we will choose the default one, such as DOID is the default identifier for Disease ontology.

In the following, we will use the Disease ontology as an example to explain how to convert identifiers among different databases. 

If you want to use the Disease ontology, you can use the DOID identifier. If you want to use the MESH identifier, you need to specify the ontology as MESH. The platform will automatically convert the MESH identifier to DOID identifier. The conversion is based on the following rules:

- If we get two or more results when we matched the MESH to DOID identifier, we will abandon the MESH identifier. But we don't care about the opposite situation.

- We will convert all the MESH identifiers to DOID identifiers in the final relationship file if we can. But if we cannot find a doid identifier for a MESH identifier, we will use the MESH identifier.

- We will also provide a dictionary for the conversion among different identifiers.