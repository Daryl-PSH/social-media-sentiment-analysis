from cassandra.cluster import Cluster, Session
import pandas as pd


def pandas_factory(colnames, rows) -> pd.DataFrame:
    """
    Helper function to extract data from Cassandra in a pandas DataFrame format
    Source: https://stackoverflow.com/questions/41247345/python-read-cassandra-data-into-pandas

    Args:
        colnames ([type]): [description]
        rows ([type]): [description]

    Returns:
        DataFrame
    """
    return pd.DataFrame(rows, columns=colnames)


def connect_to_database(keyspace: str) -> Session:
    """
    Connect to a Cassandra keyspace

    Args:
        keyspace (str): keyspace to connect to
    """
    cluster = Cluster()
    session = cluster.connect(keyspace)
    session.row_factory = pandas_factory
    session.default_fetch_size = None

    return session


def extract_data(session: Session, query: str) -> pd.DataFrame:
    """
    Extract all the data from a particular table in Cassandra

    Args:
        session (Session): Database session
        query (str): Query to be executed

    Returns:
        DataFrame: Data from Cassandra in Pandas DataFrame format
    """
    rows = session.execute(query, timeout=None)
    df = rows._current_rows

    return df
