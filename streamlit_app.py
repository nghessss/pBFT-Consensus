import streamlit as st
import os
import sys

from core.cluster import Cluster
from ui.cluster_view import render_cluster_html
from ui.sidebar import render_sidebar

st.set_page_config(page_title="Raft Simulator", layout="wide")

cluster = Cluster(num_nodes=7)

st.title("🛠️ Raft Consensus Simulator")

# Sidebar
render_sidebar(cluster)

# Main cluster view
st.subheader("Cluster View")
html = render_cluster_html(cluster.nodes)
st.components.v1.html(html, height=600)
