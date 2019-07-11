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


identifier = 'moviesGraph'
uri_base = 'http://www.ourmoviedb.org/'
filename = 'data/title.basics.tsv'
delete_existing_persistent_graph=True

# Create graph
g = Graph(store='Sleepycat', identifier=identifier)
store_dir = 'stores/Store_' + identifier

if delete_existing_persistent_graph and os.path.exists(store_dir):
  shutil.rmtree(store_dir)
g.open(store_dir,  create=True)

n = Namespace(uri_base)
genre_namespace = Namespace(uri_base + 'Genre/')

# Count lines
total_entries = num_lines = sum(1 for line in open(filename))
total_entries = round(total_entries/100) # Test
output = 'outs/' + identifier + str(total_entries) + '.ttl.n3'


progress = Progress(total_entries)
start = time.time()


with open(filename) as fd:
  genres_set = set()
  data = csv.DictReader(fd, delimiter="\t", quotechar='"', escapechar='')
  for row in data:

    # Create a node for our movie
    movie_node = n['Movie/' + row['tconst']] # Unique URI
    class_node = n['Movie']
    g.add((movie_node, n.hasOriginalTitle, Literal(row['originalTitle'])))
    g.add((movie_node, n.hasAdultContent, Literal(row['isAdult'],  datatype=XSD.boolean)))
    g.add((movie_node, RDF.type, class_node)) 

    if is_specified(row['startYear']):
      start_year_node = n['Year/' + row['startYear']]
      g.add((movie_node, n.hasStartYear, start_year_node)) 

    # Check if endYear exist
    if is_specified(row['endYear']):
      end_year_node = n['Year/' + row['endYear']]
      g.add((movie_node, n.hasEndYear, end_year_node)) 

    if is_specified(row['runtimeMinutes']):
      g.add((movie_node, n.hasRuntime, Literal(row['runtimeMinutes'],  datatype=XSD.int)))

    # Process genres
    if is_specified(row['genres']):
      genres = [x.strip() for x in row['genres'].split(',')]
      for genre in genres:
        genre_node = genre_namespace[genre] 
        g.add((movie_node, n.hasGenre, genre_node))
        genres_set.add(genre)

    # Produce rdf statements for the genres we accumulated
    if len(genres_set):
      for genre in genres_set:
        genre_node = genre_namespace[genre] 
        g.add((genre_node, n.hasGenreName, Literal(genre)))
        g.add((genre_node, RDF.type, n.Genre)) 


    progress.count()
    if progress.finished():
      break


print(len(genres_set))

g.serialize(destination=output, format='turtle')
end = time.time()
print("Total Time: ", end - start)
g.close()