# From Data Engineering to Knowledge Engineering

Code companion to the **From Data Engineering to Knowledge Engineering** article series on Substack — exploring how data engineers can build knowledge graphs using [maplib](https://github.com/DataTreehouse/maplib), [Polars](https://pola.rs/) and Python.

### The Substack article series

* [Part 0: Why should you care about Knowledge Graphs](https://substack.com/home/post/p-188465277)
* [Part 1: From Data Engineering to Knowledge Engineering](https://veronahe.substack.com/p/from-data-engineering-to-knowledge)
* [Part 2: Data Engineering Ontologies](https://substack.com/home/post/p-184015627)
* [Part 3: SPARQL for SQL Developers](https://veronahe.substack.com/p/sparql-for-sql-developers-a-translation)
* [Part 4: From SQL Constraints to SHACL Shapes](https://substack.com/home/post/p-185832762)

### What's in this repo

| Folder | Description |
|--------|-------------|
| [`small-demo/`](small-demo/) | A minimal example based on the planets/satellites dataset from Parts 1–2. Good for getting started quickly. |
| [`full-demo/`](full-demo/) | A comprehensive demonstrator with customer data from four source formats (Parquet, CSV, Excel, SQL), covering the full pipeline: OTTR templates, ontology with FIBO alignment, SKOS vocabularies, DCAT catalog, Datalog inference, SHACL validation, and 8 SPARQL competency questions. Includes a step-by-step Jupyter notebook. |

### Getting started

```bash
pip install maplib polars fastexcel
```

For the small demo:

```bash
cd small-demo
python knowledge_engineering.py
```

For the full demo:

```bash
cd full-demo
python knowledge_engineering.py
```

Or open the notebook at `full-demo/notebook/from_de_to_ke.ipynb` for a guided walkthrough.

### Resources

* [maplib tutorial documentation](https://datatreehouse.github.io/documentation/#/page/maplib%20docs)
* [maplib API documentation](https://datatreehouse.github.io/maplib/maplib.html)
* [OTTR masterclass](https://github.com/veleda/ottr-masterclass)
