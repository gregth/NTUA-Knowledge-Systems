from rdflib.graph import ConjunctiveGraph as Graph
from rdflib.store import Store
from rdflib.plugin import get as plugin
from rdflib.term import URIRef
from rdflib import OWL, RDFS, Literal, Namespace 
from rdflib.namespace import FOAF, RDF, XSD
import csv, os, time, shutil
from progress import Progress
from utils import is_specified, uri_base

identifier = 'crewGraph'
filename = 'data/title.crew.tsv'
batches = 100

# Create graph
g = Graph(identifier=identifier)
n = Namespace(uri_base)

# Count lines
total_entries = num_lines = sum(1 for line in open(filename)) - 1
total_entries = 34343

progress = Progress(total_entries, batches)
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

    if progress.is_batch_complete(): 
      output = 'outs/' + identifier + str(total_entries) + 'b' + '{num:0{width}}'.format(num=progress.current_batch, width=4) + '.ttl.n3'
      g.serialize(destination=output, format='turtle')
      g.close()
      g = Graph(identifier=identifier)

    if progress.finished():
      break
    
print("Total Items Processed: ", progress.total)
end = time.time()
print("Total Time: ", end - start)
g.close()