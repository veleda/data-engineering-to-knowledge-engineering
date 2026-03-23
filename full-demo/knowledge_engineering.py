from maplib import Model
from maplib import explore
from utils import print_count

import data_engineering as data
import time


# Serialise data frames to RDF using OTTR templates for customer data
with open("tpl/tpl.ttl", "r") as file:
    tpl = file.read()

m = Model()
m.add_template(tpl)

# Kick-start building templates with expand_default
#tmp_tpl = m.map_default(data.customers(), "customer_iri")
#print(tmp_tpl)

m.map(data.ns_tpl + "Customer", data.customers())
m.map(data.ns_tpl + "Order", data.orders())
m.map(data.ns_tpl + "Product", data.products())
m.map(data.ns_tpl + "SupportTicket", data.support_tickets())

print_count("mapping", m)


####################################### MERGE IN ONTOLOGY

m.read("ttl/ontology.ttl")
print_count("merge with ontology", m)


####################################### MERGE SKOS VOCABULARIES & DCAT CATALOG
# SKOS (Simple Knowledge Organisation System) concept schemes provide
# controlled vocabularies for product categories, customer segments,
# issue types and priorities — reusable, standardised, machine-readable.
# https://www.w3.org/TR/skos-reference/

m.read("ttl/vocab.ttl")
print_count("merge with SKOS vocabularies", m)

# DCAT (Data Catalog Vocabulary) describes the four data sources as a
# catalog — format, access URL, descriptions. Metadata that travels
# with the graph.
# https://www.w3.org/TR/vocab-dcat-3/

m.read("ttl/catalog.ttl")
print_count("merge with DCAT catalog", m)


####################################### LINK INSTANCES TO SKOS CONCEPTS
# The source data has plain strings ("Software", "Enterprise", etc.).
# The SKOS vocabulary defines proper concepts with IRIs and definitions.
# These INSERT queries bridge the two by matching on skos:prefLabel,
# creating typed object property links (:hasCategory, :hasSegment, etc.)

skos_queries = [
    "queries/link_categories_to_skos.rq",    # Product -> SKOS category
    "queries/link_segments_to_skos.rq",       # Customer -> SKOS segment
    "queries/link_issues_to_skos.rq",         # Ticket -> SKOS issue type
    "queries/link_priorities_to_skos.rq",     # Ticket -> SKOS priority
]

for qf in skos_queries:
    with open(qf, "r") as file:
        m.update(file.read())

print_count("SKOS concept linking", m)


####################################### INSERT

with open("queries/insert_customers_to_crm.rq", "r") as file:
    insert_customers_to_crm = file.read()
#m.insert(insert_customers_to_crm)

# Inserts explicit NamedIndividual facts
with open("queries/insert_individual.rq", "r") as file:
    insert_individual = file.read()
#m.update(insert_individual)

#print_count("insert queries", m)


####################################### RULES

# NB! Both m.infer() and m.validate() are functions of maplib enterpise.
# However, personal exploration is always free. Reach out to get a license.


with open("ttl/rule.dlog", "r") as file:
    rules = file.read()

#m.infer(rules)
#print_count("inference", m)


####################################### VALIDATION

#m.read("ttl/sh.ttl", graph=data.ns_sh)
#report = m.validate(shape_graph=data.ns_sh)

#df = report.results()
#print(df)

#print(report.performance)

with open("queries/focus_node_violations.rq", "r") as file:
    focus_node_violations = file.read()

#print(m.query(focus_node_violations)["focusNode"])

####################################### EXPLORE

# Run the Treehouse explorer

#m.explore(port="1234")
#time.sleep(222)

####################################### WRITE TO FILE

p = {"": "http://data.treehouse.example/"}
m.write("ttl/out.ttl", format="turtle", prefixes=p)
