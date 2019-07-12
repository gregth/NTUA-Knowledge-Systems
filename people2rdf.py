from rdflib.graph import ConjunctiveGraph as Graph
from rdflib.store import Store
from rdflib.plugin import get as plugin
from rdflib.term import URIRef
from rdflib import OWL, RDFS, Literal, Namespace
from rdflib.namespace import FOAF, RDF, XSD
import csv, os, time, shutil
from progress import Progress

def is_specified(val):
  unspecified_indicators = ['\\N']
  return val not in unspecified_indicators and not None


identifier = 'peopleGraph'
uri_base = 'http://www.ourmoviedb.org/'
filename = 'data/name.basics.tsv'
delete_existing_persistent_graph=True

# Create graph
g = Graph(store='Sleepycat', identifier=identifier)
store_dir = 'stores/Store_' + identifier
if delete_existing_persistent_graph and os.path.exists(store_dir):
  shutil.rmtree(store_dir)
g.open(store_dir,  create=True)
n = Namespace(uri_base)

# Count lines
total_entries = num_lines = sum(1 for line in open(filename))
# total_entries = 10000
output = 'outs/' + identifier + str(total_entries) + '.ttl.n3'


progress = Progress(total_entries)
start = time.time()
with open(filename) as fd:
  data = csv.DictReader(fd, delimiter="\t", quotechar='"', escapechar='')
  for row in data:

    # Create a node for our person
    person_node = n['Person/' + row['nconst']] # Unique URI
    class_node = n['Person']
    g.add((person_node, n.hasID, Literal(row['nconst'])))
    g.add((person_node, n.hasPrimaryName, Literal(row['primaryName'])))
    g.add((person_node, RDF.type, class_node))

    if is_specified(row['birthYear']):
      birth_year_node = n['Year/' + row['birthYear']]
      g.add((person_node, n.hasBirthYear, birth_year_node))

    if is_specified(row['deathYear']):
      death_year_node = n['Year/' + row['deathYear']]
      g.add((person_node, n.hasDeathYear, death_year_node))

    # Write down the primary professions specified in the database
    if is_specified(row['primaryProfession']):
      professions = [x.strip() for x in row['primaryProfession'].split(',')]
      for p in professions:
        # TODO make professions instances maybe?
        g.add((person_node, n.hasProfession, Literal(p)))

    # Keep record of the tittles the actor is known for
    if is_specified(row['knownForTitles']):
      titles = [x.strip() for x in row['knownForTitles'].split(',')]
      for t in titles:
        movie_node = n['Movie/'+ t]
        g.add((person_node, n.isKnownFor, movie_node))

    progress.count()
    if progress.finished():
      break

g.serialize(destination=output, format='turtle')
end = time.time()
print("Total Time: ", end - start)
g.close()
