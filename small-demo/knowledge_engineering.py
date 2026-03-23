from maplib import Model

import data_engineering as de

m = Model()
m.map_default(de.planets(), "planet_iri")
m.map_default(de.satellites(), "satellite_iri")

# read and run query selecting everything (*)
with open("queries/select_all_the_things.rq", "r") as file:
    select_all_the_things = file.read()
everything = m.query(select_all_the_things)
#print(everything)

# read in and merge the ontology
m.read("rdf/ast.rdf", format="rdf/xml")

# updating our knowledge graph with connecting our
# planet data with the ontology
with open("queries/add_planet_fact.rq", "r") as file:
    add_planet_fact = file.read()
m.update(add_planet_fact)

# query selecting all planets, using the ontology class for planet
with open("queries/select_all_planets.rq", "r") as file:
    select_all_planets = file.read()
all_planets = m.query(select_all_planets)
#print(all_planets)

# selecting planets and counting all their moons
with open("queries/count_moons.rq", "r") as file:
    count_moons = file.read()
planets_with_num_moons = m.query(count_moons)
print(planets_with_num_moons)
