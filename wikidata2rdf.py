from rdflib.graph import ConjunctiveGraph as Graph
from rdflib.store import Store
from rdflib.plugin import get as plugin
from rdflib.term import URIRef
from rdflib import OWL, RDFS, Literal, Namespace 
from rdflib.namespace import FOAF, RDF, XSD
import csv, os, time, shutil
from progress import Progress
from utils import is_specified, uri_base
import re

identifier = 'wikiGraph'
filename = 'data/wikiFetchTitles.tsv'

# Create graph
g = Graph(identifier=identifier)
n = Namespace(uri_base)

# Count lines
total_entries = num_lines = sum(1 for line in open(filename)) - 1

progress = Progress(total_entries)
start = time.time()
wrongs = []
with open(filename) as fd:
  data = csv.DictReader(fd, delimiter="\t", quotechar='"', escapechar='')
  for r in data:
    raw_id = r['raw_id']

    # Check if valid with regex
    match = re.match(r"^(tt)*(?P<id>\d{7,10}).*", raw_id)
    if not match:
      progress.count()
      wrongs.append(raw_id)
      continue

    imdb_id = match.group(2)
    film_node = n['Movie/tt' + imdb_id]

    # Create a node for dbpedia
    uri = r['uri']
    wiki_node = URIRef(uri)
    g.add((film_node, n.hasWikiNode, wiki_node))

    progress.count()
    if progress.finished():
      break
    
output = 'outs/' + identifier + str(total_entries) + '.ttl.n3'
g.serialize(destination=output, format='turtle')
end = time.time()

print('Wrong found: ', len(wrongs))
print(wrongs)
print("Total Items Processed: ", progress.total)
print("Total Time: ", end - start)
g.close()