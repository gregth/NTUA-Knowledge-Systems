import csv, os, time, shutil
from SPARQLWrapper import SPARQLWrapper, JSON
import re
import time

def get_movies_from_dbpedia(limit, offset):
  e = 'https://query.wikidata.org/bigdata/ldf'
  i = "https://query.wikidata.org/sparql"
  wikidata = SPARQLWrapper(i)
  wikidata.setQuery("""
      SELECT ?film ?id
      WHERE {
        ?film wdt:P31 wd:Q11424.
        ?film p:P345 ?imdb_entity.
        ?imdb_entity ps:P345 ?id.
      }
      ORDER BY ?id
      LIMIT """ + str(limit)  + """
      OFFSET  """ + str(offset) + """ 
    """)
  wikidata.setReturnFormat(JSON)
  results = wikidata.query().convert()
  return results['results']['bindings']


offset = 0
limit = 200000
out_file = 'data/wikiFetchTitles.tsv'

with open(out_file, 'wt') as out_file:
  tsv_writer = csv.writer(out_file, delimiter='\t')
  tsv_writer.writerow(['raw_id', 'uri'])
  batch = 0
  total_results = 0
  while True:
    print("Requesting batch no: ", batch)
    results = get_movies_from_dbpedia(limit, offset)
    print('Offset: ', offset)
    print("Received results count: " + str(len(results)))
    if not len(results):
      break
    for r in results:
      raw_id = r['id']['value']
      uri = r['film']['value']
      total_results += 1
      tsv_writer.writerow([raw_id, uri])

    offset += len(results)
    batch += 1
    print("Sleeping 60 seconds to avoid too many requests error...")
    time.sleep(60)

print('Total results fetched: ', total_results)