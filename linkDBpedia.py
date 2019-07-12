from rdflib.graph import ConjunctiveGraph as Graph
from rdflib.store import Store
from rdflib.plugin import get as plugin
from rdflib.term import URIRef
from rdflib import OWL, RDFS, Literal, Namespace 
from rdflib.namespace import FOAF, RDF, XSD
import csv, os, time, shutil
from progress import Progress
from SPARQLWrapper import SPARQLWrapper, JSON
from urllib.parse import urlencode
import re


def get_movies_from_dbpedia(limit, offset):
  dbpedia = SPARQLWrapper("http://dbpedia.org/sparql")
  dbpedia.setQuery("""
      PREFIX dbo: <http://dbpedia.org/ontology/>
      SELECT ?film_uri ?id
      WHERE {
        ?film_uri a dbo:Film;
        dbo:imdbId ?id .
      }
      ORDER BY ?id
      LIMIT """ + str(limit)  + """
      OFFSET  """ + str(offset) + """ 
    """)
  dbpedia.setReturnFormat(JSON)
  results = dbpedia.query().convert()
  return results["results"]["bindings"]


def get_total_dbpedia_results():
  total_results = []
  offset = 0
  while True:
    results = get_movies_from_dbpedia(5000, offset)
    print("Received results count: " + str(len(results)))
    if not len(results):
      break
    for r in results:
      total_results.append(r)
    offset += len(results)
  print('Total results: ', len(total_results))
  return total_results



identifier = 'dbpediaLinksGraph'
uri_base = 'http://www.ourmoviedb.org/'
output = 'outs/' + identifier + '.ttl.n3'
delete_existing_persistent_graph=True

# Create graph
g = Graph(store='Sleepycat', identifier=identifier)
store_dir = 'stores/Store_' + identifier

if delete_existing_persistent_graph and os.path.exists(store_dir):
  shutil.rmtree(store_dir)
g.open(store_dir,  create=True)

entries = get_total_dbpedia_results()
total_entries = len(entries) 

progress = Progress(total_entries)
start = time.time()

n = Namespace(uri_base)

wrongs = 0
for r in entries:
  raw_id = r['id']['value']
  match = re.match(r"^(tt)*(?P<id>\d{7,10}).*", raw_id)
  if not match:
    wrongs += 1
    continue
  imdb_id = match.group(2)
  film_node = n['Movie/tt' + imdb_id]

  # Create a node for dbpedia
  uri = r['film_uri']['value']
  dbp_node = URIRef(uri)
  g.add((film_node, n.hasDBPNode, dbp_node))
  progress.count()
    
print("Not valid IMDB ids: ", wrongs)
g.serialize(destination=output, format='turtle')
end = time.time()
print("Total Time: ", end - start)
g.close()