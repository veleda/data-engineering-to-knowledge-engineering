# From Data Engineering to Knowledge Engineering

Woho! 💥

This repo is the practical culmination of the **From Data Engineering to Knowledge Engineering** blog series on Substack. Each article introduced one piece of the puzzle — this project puts them all together into a runnable demonstrator using [maplib](https://github.com/DataTreehouse/maplib), [Polars](https://pola.rs/) and Python.

### The Substack article series

* [Part 0: Why should you care about Knowledge Graphs](https://substack.com/home/post/p-188465277)
* [Part 1: From Data Engineering to Knowledge Engineering](https://veronahe.substack.com/p/from-data-engineering-to-knowledge)
* [Part 2: Data Engineering Ontologies](https://substack.com/home/post/p-184015627)
* [Part 3: SPARQL for SQL Developers](https://veronahe.substack.com/p/sparql-for-sql-developers-a-translation)
* [Part 4: From SQL Constraints to SHACL Shapes](https://substack.com/home/post/p-185832762)

### Data sources

This masterclass demonstrates loading data from **four different source types**, reflecting real-world data engineering patterns:

| Source | Format | File | Description |
|--------|--------|------|-------------|
| Customers | **Parquet** | `data/customers.parquet` | 25 customers with name, email, country, segment, signup date. Parquet is the default columnar format in Databricks / Spark lakehouse architectures. |
| Orders | **CSV** | `data/orders.csv` | 60 orders with customer/product references, quantities, amounts, dates, status. CSV is the classic flat-file exchange format. |
| Products | **Excel** | `data/products.xlsx` | 15 products with name, category, price, stock. Excel is a typical business-managed data source. |
| Support Tickets | **SQL** | `data/support_tickets.csv` | 40 support tickets queried via `Polars SQLContext`, simulating a SQL endpoint (Databricks SQL, Snowflake, Postgres, etc.). |

### Masterclass contents

| Folder | Content |
|--------|---------|
| `data` | parquet, csv, and xlsx files with customer, order, product, and support ticket data |
| `queries` | a few SPARQL queries |
| `tpl` | [OTTR](https://ottr.xyz/) templates for Customer, Order, Product and SupportTicket |
| `ttl` | all RDF turtle files (ontology, shapes, output, validation report), including datalog rules |

* `utils.py` contains helper functions as `print_count(msg: str, Model)`.
* `data_engineering.py` contains static variables for namespaces and functions for data engineering with data frames for instance data. Demonstrates reading from parquet, CSV, Excel, and SQL — including **entity harmonization** across sources with different naming conventions.
* `knowledge_engineering.py` where the magic happens ✨ --- maplib knowledge graph construction, including querying, validation and reasoning.
* `notebook/from_de_to_ke.ipynb` — step-by-step Jupyter notebook walking through the full knowledge engineering pipeline.

### Entity harmonization

In real organisations, the same entity appears across multiple source systems — each with its own naming conventions. This masterclass deliberately mirrors that reality:

| Concept | customers.parquet | orders.csv | products.xlsx | support_tickets |
|---------|------------------|------------|--------------|-----------------|
| Customer FK | `customer_id` | `cust_ref` | — | `client_id` |
| Name | `full_name` | — | `name` | — |
| Status | — | `order_status` | — | `state` |
| Price | — | — | `price` | — |
| Date | `registered_at` | `order_date` | — | `date_created` |

Each function in `data_engineering.py` harmonizes its source's column names to a shared vocabulary before constructing IRIs. This ensures that the same customer gets the same IRI regardless of which source mentions them — and from that point on, the graph connects everything automatically.

See `data.column_mapping_table()` for the full mapping, or Section 1b of the notebook for a walkthrough.

### Domain model

The ontology (`ttl/ontology.ttl`) models a simple CRM domain:

* **Customer** — with email, country, segment, signup date
* **Order** — placed by a customer, contains a product, with quantity, amount, date, status
* **Product** — with category, unit price, stock quantity
* **SupportTicket** — raised by a customer, with issue type, priority, status

Inference rules (`ttl/rule.dlog`) derive:

* Inverse relationships: `hasOrder`, `hasTicket` (from `placedBy`, `raisedBy`)
* Transitive purchasing: if a customer placed an order containing a product → `purchased`
* Product classification: `PremiumProduct` (price > 500) and `BudgetProduct` (price ≤ 100)

### External ontologies

The masterclass connects to three well-known W3C/industry standards, demonstrating reuse of existing knowledge:

| Standard | File | What it provides |
|----------|------|------------------|
| [SKOS](https://www.w3.org/TR/skos-reference/) | `ttl/vocab.ttl` | Controlled vocabularies (ConceptSchemes) for product categories, customer segments, issue types and priorities. Instances are linked via INSERT queries in `queries/link_*_to_skos.rq`. |
| [DCAT](https://www.w3.org/TR/vocab-dcat-3/) | `ttl/catalog.ttl` | A data catalog describing the four source datasets — format, access URL, descriptions. Metadata that travels with the graph. |
| [FIBO](https://spec.edmcouncil.org/fibo/) | `ttl/ontology.ttl` | Financial Industry Business Ontology links: `Customer` → `fibo:AgentInRole`, `Order` → `fibo:MutualCommitment`, `Product` → `fibo:Product`. |

### Competency Questions (SPARQL)

The `queries/` folder contains 8 competency questions that showcase what a knowledge graph + SPARQL can do that SQL alone cannot (or can only do painfully). Each `.rq` file includes a SQL equivalent as a comment.

| CQ | File | Question | Why SPARQL shines |
|----|------|----------|-------------------|
| 1 | `cq1.rq` | Which products has each customer purchased? | Queries the **inferred** `:purchased` relationship — no JOINs needed, the reasoning engine did the work |
| 2 | `cq2.rq` | Which customers purchased a PremiumProduct but also raised a critical support ticket? | Chains **two separate inferences** (product classification + transitive purchase) with a third entity type |
| 3 | `cq3.rq` | Which customers have orders but never raised a support ticket? | `FILTER NOT EXISTS` — elegant negation that reads like English |
| 4 | `cq4.rq` | What classes exist, what properties belong to each, and how many instances? | **Schema introspection** — the schema IS data; SQL can't mix `information_schema` with instance queries |
| 5 | `cq5.rq` | Customer 360° view — orders, products, product classifications, tickets in one query | Composable `OPTIONAL` blocks avoid the **Cartesian explosion** that plagues multi-LEFT-JOIN SQL |
| 6 | `cq6.rq` | Which customers purchased products from ALL categories? | **Relational division** — notoriously awkward in SQL (double-NOT-EXISTS), clean in SPARQL |
| 7 | `cq7.rq` | CONSTRUCT a customer 360° subgraph | Returns a **graph**, not a table — SQL simply cannot do this |
| 8 | `cq8.rq` | Revenue per country per product category (only categories with a PremiumProduct) | Traverses 4 entity types with **inferred classification** and `FILTER EXISTS` for category filtering |

### Resources

* [Tutorial documentation](https://datatreehouse.github.io/documentation/#/page/maplib%20docs)
* [maplib API documentation](https://datatreehouse.github.io/maplib/maplib.html)
* [OTTR masterclass](https://github.com/veleda/ottr-masterclass)
