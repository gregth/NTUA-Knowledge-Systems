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


identifier = 'principalsGraph'
uri_base = 'http://www.ourmoviedb.org/'
filename = 'data/title.principals.tsv'
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
    person_node = n['Person/' + row['nconst']]

    role = row['category']
    # Define correspodencies between input data and property names
    correspodencies = {
      'actor' : 'hasActor',
      'actress' : 'hasActor',
      'producer' : 'hasProducer',
      'writer' : 'hasWriter',
      'composer' : 'hasComposer',
      'self' : 'hasSelfAppearance',
      'cinematographer' : 'hasCinematographer',
      'director' : 'hasDirector',
      'production_designer' : 'hasProductionDesigner',
      'editor' : 'hasEditor',
      'archive_footage' : 'archive_footage_by',
      'archive_sound' : 'archive_sound_by',
    }

    if role in correspodencies:
      g.add((film_node, n[correspodencies[role]], person_node))
    else:
      other_roles.add(role)

    progress.count()
    if progress.finished():
      break
    
# Let's find out if we missed any roles
print(other_roles)
      
g.serialize(destination=output, format='turtle')
end = time.time()
print("Total Time: ", end - start)
g.close()