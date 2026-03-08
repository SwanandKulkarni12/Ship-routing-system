import os
import math
import logging
import networkx as nx
import numpy as np
from sklearn.neighbors import BallTree
from graph_loader import haversine_distance
logger = logging.getLogger(__name__)
HEX_LATTICE_ENABLED = os.getenv('HEX_LATTICE_ENABLED', 'true').strip().lower() in {'1', 'true', 'yes', 'on'}
HEX_LATTICE_SPACING_DEG = float(os.getenv('HEX_LATTICE_SPACING_DEG', '0.30'))
HEX_LATTICE_MAX_POINTS = int(os.getenv('HEX_LATTICE_MAX_POINTS', '2200'))
HEX_LATTICE_ATTACH_K = int(os.getenv('HEX_LATTICE_ATTACH_K', '3'))
HEX_LATTICE_ATTACH_MAX_KM = float(os.getenv('HEX_LATTICE_ATTACH_MAX_KM', '35.0'))
HEX_LATTICE_NEIGHBOR_KM = float(os.getenv('HEX_LATTICE_NEIGHBOR_KM', '65.0'))

def _haversine_km(node_a, node_b):
    lon1, lat1 = (float(node_a[0]), float(node_a[1]))
    lon2, lat2 = (float(node_b[0]), float(node_b[1]))
    distance = haversine_distance(lat1, lon1, np.array([lat2]), np.array([lon2]))
    return float(distance[0])

def _hex_points_for_bbox(min_lon, max_lon, min_lat, max_lat, spacing_deg):
    if max_lon <= min_lon or max_lat <= min_lat:
        return []
    spacing = max(float(spacing_deg), 0.05)
    lat_step = spacing * math.sqrt(3) / 2.0
    points = []
    row = 0
    lat = min_lat
    while lat <= max_lat:
        lon_offset = spacing / 2.0 if row % 2 == 1 else 0.0
        lon = min_lon + lon_offset
        while lon <= max_lon:
            points.append((round(float(lon), 6), round(float(lat), 6)))
            lon += spacing
        lat += lat_step
        row += 1
    if len(points) <= HEX_LATTICE_MAX_POINTS:
        return points
    stride = max(1, int(np.ceil(len(points) / HEX_LATTICE_MAX_POINTS)))
    return points[::stride][:HEX_LATTICE_MAX_POINTS]

def _augment_subgraph_with_hex_lattice(base_subgraph, a_star_path):
    if not HEX_LATTICE_ENABLED:
        return base_subgraph
    if base_subgraph.number_of_nodes() < 10:
        return base_subgraph
    if not a_star_path:
        return base_subgraph
    lons = [float(p[0]) for p in a_star_path]
    lats = [float(p[1]) for p in a_star_path]
    if not lons or not lats:
        return base_subgraph
    pad = max(HEX_LATTICE_SPACING_DEG * 1.2, 0.12)
    min_lon, max_lon = (min(lons) - pad, max(lons) + pad)
    min_lat, max_lat = (min(lats) - pad, max(lats) + pad)
    hex_points = _hex_points_for_bbox(min_lon, max_lon, min_lat, max_lat, HEX_LATTICE_SPACING_DEG)
    if not hex_points:
        return base_subgraph
    base_nodes = list(base_subgraph.nodes())
    base_latlon = np.array([(float(lat), float(lon)) for lon, lat in base_nodes], dtype=np.float64)
    base_tree = BallTree(np.radians(base_latlon), metric='haversine')
    query_latlon = np.array([(float(lat), float(lon)) for lon, lat in hex_points], dtype=np.float64)
    d_rad, idx = base_tree.query(np.radians(query_latlon), k=min(max(1, HEX_LATTICE_ATTACH_K), len(base_nodes)))
    d_km = d_rad * 6371.0
    kept_points = []
    kept_indices = []
    for i in range(len(hex_points)):
        if d_km[i, 0] <= HEX_LATTICE_ATTACH_MAX_KM:
            kept_points.append(hex_points[i])
            kept_indices.append(i)
    if not kept_points:
        return base_subgraph
    augmented = nx.Graph(base_subgraph)
    for node in kept_points:
        augmented.add_node(node)
    hex_latlon = np.array([(float(lat), float(lon)) for lon, lat in kept_points], dtype=np.float64)
    hex_tree = BallTree(np.radians(hex_latlon), metric='haversine')
    r_rad = HEX_LATTICE_NEIGHBOR_KM / 6371.0
    neighbor_sets = hex_tree.query_radius(np.radians(hex_latlon), r=r_rad, return_distance=True, sort_results=True)
    for i in range(len(kept_points)):
        nbr_idx_arr, nbr_dist_arr = (neighbor_sets[0][i], neighbor_sets[1][i])
        u = kept_points[i]
        for j, d_rad_uv in zip(nbr_idx_arr, nbr_dist_arr):
            if j <= i:
                continue
            v = kept_points[int(j)]
            d_km_uv = float(d_rad_uv) * 6371.0
            if d_km_uv <= 0.0:
                continue
            augmented.add_edge(u, v, weight=d_km_uv, distance=d_km_uv)
    for kept_i, orig_i in enumerate(kept_indices):
        u = kept_points[kept_i]
        for rank in range(min(d_km.shape[1], max(1, HEX_LATTICE_ATTACH_K))):
            d = float(d_km[orig_i, rank])
            if d > HEX_LATTICE_ATTACH_MAX_KM:
                continue
            v = base_nodes[int(idx[orig_i, rank])]
            if u == v:
                continue
            d_uv = _haversine_km(u, v)
            augmented.add_edge(u, v, weight=d_uv, distance=d_uv)
    logger.info('[subgraph-builder] hex lattice added nodes=%s total_nodes=%s total_edges=%s', len(kept_points), augmented.number_of_nodes(), augmented.number_of_edges())
    return augmented

def build_subgraph(G, tree, node_array, a_star_path, radius_km=700):
    earth_radius_km = 6371
    radius_rad = radius_km / earth_radius_km
    subgraph_nodes = set()
    node_list = list(G.nodes())
    for lon, lat in a_star_path:
        query_rad = np.radians([[lat, lon]])
        indices = tree.query_radius(query_rad, r=radius_rad)[0]
        candidates = node_array[indices]
        distances = haversine_distance(lat, lon, candidates[:, 0], candidates[:, 1])
        close_indices = indices[distances <= radius_km]
        for idx in close_indices:
            subgraph_nodes.add(node_list[idx])
    logger.info('[subgraph-builder] selected_nodes=%s radius_km=%s', len(subgraph_nodes), radius_km)
    subgraph_nodes.update(a_star_path)
    base_subgraph = G.subgraph(subgraph_nodes).copy()
    return _augment_subgraph_with_hex_lattice(base_subgraph, a_star_path)