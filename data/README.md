### Download Ontology List From `bioportal.bioontology.org`

You need to register for an account at [bioportal.bioontology.org](https://bioportal.bioontology.org/) and get an API key.  Then you can download the list of ontologies with the following command:

```
curl -X GET -H "Authorization: apikey token=<your-token-here>" http://data.bioontology.org/ontologies -o ontologies.json
```

