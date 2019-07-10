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
  return val not in unspecified_indicators


identifier = 'crewGraph'
uri_base = 'http://www.ourmoviedb.org/'
filename = 'data/title.crew.tsv'
output = 'outs/' + identifier + '.ttl.n3'
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
total_entries = round(total_entries/200) # Test

progress = Progress(total_entries)
start = time.time()


with open(filename) as fd:
  data = csv.DictReader(fd, delimiter="\t", quotechar='"', escapechar='')
  other_roles = set()

  for row in data:
    film_node = n['Movie/' + row['tconst']]

    # Record writers of each movie
    writers_string = row['writers']
    if is_specified(writers_string):
      writers = [x.strip() for x in writers_string.split(',')]
      for w in writers:
        writer_node = n['Person/' + w]
        g.add((film_node, n.hasWriter, writer_node))

    # Record direcors of each movie
    directors_string = row['directors']
    if is_specified(directors_string):
      directors = [x.strip() for x in directors_string.split(',')]
      for d in directors:
        director_node = n['Person/' + d]
        g.add((film_node, n.hasDirector, director_node))

    progress.count()
    if progress.finished():
      break
    
g.serialize(destination=output, format='turtle')
end = time.time()
print("Total Time: ", end - start)
g.close()