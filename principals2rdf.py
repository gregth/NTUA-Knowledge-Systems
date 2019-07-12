from rdflib.graph import ConjunctiveGraph as Graph
from rdflib.store import Store
from rdflib.plugin import get as plugin
from rdflib.term import URIRef
from rdflib import OWL, RDFS, Literal, Namespace
from rdflib.namespace import FOAF, RDF, XSD
import csv, os, time, shutil
from progress import Progress
from utils import uri_base, is_specified


identifier = 'principalsGraph'
filename = 'data/title.principals.tsv'
batches = 100

# Create graph
g = Graph(identifier=identifier)
n = Namespace(uri_base)

# Count lines
total_entries = num_lines = sum(1 for line in open(filename))

progress = Progress(total_entries, batches)
start = time.time()
with open(filename) as fd:
  data = csv.DictReader(fd, delimiter="\t", quotechar='"', escapechar='')
  other_roles = set()
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
  for row in data:
    film_node = n['Movie/' + row['tconst']]
    person_node = n['Person/' + row['nconst']]

    role = row['category']
    # Define correspodencies between input data and property names


    if role in correspodencies:
      g.add((film_node, n[correspodencies[role]], person_node))
    else:
      other_roles.add(role)

    progress.count()
    # Time to write the current batch and clear the graph
    if progress.is_batch_complete(): 
      output = 'outs/' + identifier + str(total_entries) + 'b' + '{num:0{width}}'.format(num=progress.current_batch, width=4) + '.ttl.n3'
      # print('Serializing batch #', progress.current_batch)
      # print('Count #', progress.progress)
      g.serialize(destination=output, format='turtle')

      # Create graph
      g.close()
      g = Graph(identifier=identifier)

    if progress.finished():
      break

print("Total Items Processed: ", progress.total)
print(other_roles)
# Let's find out if we missed any roles
end = time.time()
print("Total Time: ", end - start)
g.close()
