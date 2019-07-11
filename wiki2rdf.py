from rdflib.graph import ConjunctiveGraph as Graph
from rdflib.store import Store
from rdflib.plugin import get as plugin
from rdflib.term import URIRef
from rdflib import OWL, RDFS, Literal, Namespace 
from rdflib.namespace import FOAF, RDF, XSD
import csv, os, time, shutil
from progress import Progress
import re

def is_specified(val):
  unspecified_indicators = ['\\N']
  return val not in unspecified_indicators


identifier = 'wikiGraph'
uri_base = 'http://www.ourmoviedb.org/'
filename = 'data/wikiFetchTitles.tsv'
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
output = 'outs/' + identifier + str(total_entries) + '.ttl.n3'

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
    
print('Wrong found: ', len(wrongs))
print(wrongs)

g.serialize(destination=output, format='turtle')
end = time.time()
print("Total Time: ", end - start)
g.close()