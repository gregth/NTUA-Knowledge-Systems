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


offset = 0
limit = 5000

batch = 0
out_file = 'data/dbpediaFetchTitles.tsv'
with open(out_file, 'wt') as out_file:
  tsv_writer = csv.writer(out_file, delimiter='\t')
  tsv_writer.writerow(['raw_id', 'uri'])
  total_results = 0
  while True:
    print("Requesting batch no: ", batch)
    results = get_movies_from_dbpedia(limit, offset)
    print('Offset: ', offset)
    print("Received results count: " + str(len(results)) + '\n')
    
    # If no more results, stop
    if not len(results):
      break

    for r in results:
      total_results += 1
      raw_id = r['id']['value']
      uri = r['film_uri']['value']
      tsv_writer.writerow([raw_id, uri])

    offset += len(results)
    batch += 1
print('Total results fetched: ', total_results)