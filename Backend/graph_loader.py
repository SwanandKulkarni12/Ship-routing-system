import networkx as nx
import re
from sklearn.neighbors import BallTree
import numpy as np
import logging
logger = logging.getLogger(__name__)
def parse_node_id(node_id):
    try:
        numbers = re.findall('-?\\d+\\.?\\d*', node_id)
        if len(numbers) != 2:
            raise ValueError('Invalid node ID format')
        return (float(numbers[0]), float(numbers[1]))
    except Exception as e:
        logger.warning('Error parsing node ID %s: %s', node_id, e)
        return None
def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat / 2) ** 2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(a))
def build_spatial_index(G):
    nodes = np.array([(lat, lon) for lon, lat in G.nodes()])
    return (BallTree(np.radians(nodes), metric='haversine'), nodes)
def _cached_node_list(G):
    cache_key = '__node_list_cache'
    cached = G.graph.get(cache_key)
    if cached is None or len(cached) != G.number_of_nodes():
        cached = list(G.nodes())
        G.graph[cache_key] = cached
    return cached
def find_k_nearest_water_nodes(G, query_coord, tree, k=1):
    k = max(1, int(k))
    query = np.radians([[query_coord[1], query_coord[0]]])
    dist_rad, idx = tree.query(query, k=min(k, G.number_of_nodes()))
    node_list = _cached_node_list(G)
    results = []
    for d_rad, i in zip(dist_rad[0], idx[0]):
        node = node_list[int(i)]
        results.append((node, float(d_rad) * 6371.0))
    return results
def find_nearest_water_node(G, query_coord, tree):
    return find_k_nearest_water_nodes(G, query_coord, tree, k=1)[0][0]
def load_navigation_graph(file_path):
    import gzip
    import os
    import pickle
    pickle_path = os.path.splitext(file_path)[0] + '.pkl.gz'
    if os.path.exists(pickle_path):
        logger.info('Loading graph from compressed pickle: %s', pickle_path)
        with gzip.open(pickle_path, 'rb') as f:
            return pickle.load(f)
    logger.info('Loading graph from GraphML (run convert_graph.py to speed this up): %s', file_path)
    G = nx.read_graphml(file_path, node_type=parse_node_id)
    return G