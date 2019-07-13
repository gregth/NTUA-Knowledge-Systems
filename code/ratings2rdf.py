from rdflib.graph import ConjunctiveGraph as Graph
from rdflib.store import Store
from rdflib.plugin import get as plugin
from rdflib.term import URIRef
from rdflib import OWL, RDFS, Literal, Namespace
from rdflib.namespace import FOAF, RDF, XSD
import csv, os, time, shutil
from progress import Progress
from utils import is_specified, uri_base


identifier = 'ratingsGraph'
filename = 'data/title.ratings.tsv'
batches = 100

# Create graph
g = Graph(identifier=identifier)
n = Namespace(uri_base)

# Count lines, don't forget to remove header
total_entries = num_lines = sum(1 for line in open(filename)) - 1

progress = Progress(total_entries, batches)
start = time.time()
with open(filename) as fd:
  data = csv.DictReader(fd, delimiter="\t", quotechar='"', escapechar='')
  other_roles = set()
  for row in data:
    film_node = n['Movie/' + row['tconst']]

    # Record writers of each movie
    rating = row['averageRating']
    g.add((film_node, n.hasAvgRating, Literal(rating, datatype=XSD.float)))

    votes = row['numVotes']
    g.add((film_node, n.votesNumber, Literal(votes, datatype=XSD.int)))

    progress.count()

    # Time to write the current batch and clear the graph
    if progress.is_batch_complete():
      output = 'outs/' + identifier + str(total_entries) + 'b' + '{num:0{width}}'.format(num=progress.current_batch, width=4) + '.ttl.n3'
      g.serialize(destination=output, format='turtle')
      g.close()
      g = Graph(identifier=identifier)

print("Total Items Processed: ", progress.total)
end = time.time()
print("Total Time: ", end - start)
g.close()