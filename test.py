from rdflib.graph import ConjunctiveGraph as Graph
from rdflib.store import Store
from rdflib.plugin import get as plugin
from rdflib.term import URIRef
from rdflib import OWL, RDFS, Literal
from rdflib.namespace import FOAF, RDF
from rdflib import Namespace
import csv
import os

# Connect to Virtuoso Server
# DSN VOS is specified in /etc/odbc.ini
import pyodbc
c = pyodbc.connect('DSN=VOS;UID=dba;PWD=virtuoso')

# A function to indicate process
def indicate_progress(progress, total, step=10000):
  if not progress:
    print("Total Entries: ", total)
  if not progress % step:
    percentage = progress / total * 100
    print("Progress:\t", progress,"\t", percentage, " %")

# Create graph
g = Graph(store='Sleepycat', identifier="mygraph")
g.open('myRDFLibStore', create=True)


uri_base = 'http://ntua.gr/ontologies/'
n = Namespace(uri_base)

# Open file and parse it
filename = 'data/title.basics.tsv'
# Efficient way to count lines
total_entries = os.stat(filename).st_size
progress = 0
total_entries = round(total_entries / 1000) # Test

with open(filename) as fd:
  data = csv.DictReader(fd, delimiter="\t", quotechar='"')
  for row in data:
    # Create a unique node
    movie_node = n['movies/' + row['tconst']]
    g.add((movie_node, n.name, Literal(row['primaryTitle'])))
    indicate_progress(progress, total_entries, step=10000)
    progress += 1
    if progress == total_entries:
      break

g.serialize(destination='output.txt', format='n3')
g.close()