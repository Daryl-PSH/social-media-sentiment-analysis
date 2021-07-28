from cassandra.cluster import Cluster
import pandas as pd


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


keyspace = "social_media"

cluster = Cluster()
session = cluster.connect(keyspace)
session.row_factory = pandas_factory
session.default_fetch_size = None

query = "SELECT * FROM tweets LIMIT 5"

rows = session.execute(query, timeout=None)
df = rows._current_rows

print(df)
