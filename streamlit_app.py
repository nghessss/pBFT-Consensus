import streamlit as st
import os
import sys
import time

from core.pbft_cluster import PBFTCluster
from ui.cluster_view import render_cluster_html
from ui.sidebar import render_sidebar

st.set_page_config(page_title="pBFT Simulator", layout="wide")

# Auto-refresh every 2 seconds when cluster is running
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()

# Initialize cluster in session state
if 'cluster' not in st.session_state:
    st.session_state.cluster = PBFTCluster(num_nodes=7)

cluster = st.session_state.cluster

st.title("🛡️ Practical Byzantine Fault Tolerance (pBFT) Simulator")

# Info banner
st.info("""
**pBFT Consensus Protocol** - Tolerates up to **f** Byzantine (malicious) faults with **3f+1** total nodes.
- 👑 **Primary** (gold border): Proposes requests
- 🔵 **Backup** (blue): Validates and votes
- ⚠️ **Byzantine** (red dashed): Malicious/faulty node
- 🔴 **Crashed** (gray): Offline node
""")

# Sidebar
render_sidebar(cluster)

# Quick action buttons in main area
st.markdown("### 🚀 Quick Actions")
col_a, col_b, col_c, col_d = st.columns(4)
with col_a:
    if st.button("📝 Submit SET x=100", use_container_width=True):
        cluster.submit_request({"type": "SET", "key": "x", "value": "100"}, client_id=1)
        st.success("Request submitted!")
        time.sleep(0.5)
        st.rerun()

with col_b:
    if st.button("📖 Submit GET x", use_container_width=True):
        cluster.submit_request({"type": "GET", "key": "x"}, client_id=1)
        st.success("Request submitted!")
        time.sleep(0.5)
        st.rerun()

with col_c:
    if st.button("🗑️ Submit DELETE x", use_container_width=True):
        cluster.submit_request({"type": "DELETE", "key": "x"}, client_id=1)
        st.success("Request submitted!")
        time.sleep(0.5)
        st.rerun()

with col_d:
    if st.button("🔁 Submit 5 Requests", use_container_width=True):
        for i in range(5):
            cluster.submit_request({"type": "SET", "key": f"key{i}", "value": f"value{i}"}, client_id=1)
        st.success("5 requests submitted!")
        time.sleep(0.5)
        st.rerun()

st.markdown("---")

# Main cluster view
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("🌐 Cluster Visualization")
    
    # Show simple grid visualization
    node_cols = st.columns(min(7, cluster.num_nodes))
    for idx, node in enumerate(cluster.nodes):
        with node_cols[idx % 7]:
            # Determine node appearance
            if not node.alive:
                emoji = "⚫"
                color = "gray"
                status = "Crashed"
            elif node.is_byzantine:
                emoji = "⚠️"
                color = "red"
                status = "Byzantine"
            elif node.is_primary():
                emoji = "👑"
                color = "gold"
                status = "Primary"
            else:
                emoji = "🔵"
                color = "blue"
                status = "Backup"
            
            st.markdown(f"""
            <div style="text-align: center; padding: 10px; border: 2px solid {color}; border-radius: 10px; margin: 5px; background-color: rgba(255,255,255,0.1);">
                <div style="font-size: 32px;">{emoji}</div>
                <div style="font-weight: bold; font-size: 18px;">Node {node.id}</div>
                <div style="font-size: 12px; color: {color};">{status}</div>
                <div style="font-size: 10px;">View {node.view}</div>
                <div style="font-size: 10px;">Seq {node.sequence_number}</div>
                <div style="font-size: 10px;">Exec {node.last_executed}</div>
            </div>
            """, unsafe_allow_html=True)
    
    # Also show detailed table
    st.markdown("---")
    st.markdown("**📋 Detailed Node Status**")
    
    import pandas as pd
    node_data = []
    for node in cluster.nodes:
        node_data.append({
            "ID": node.id,
            "Role": "👑 Primary" if node.is_primary() else "🔵 Backup",
            "Status": "✅" if node.alive else "❌",
            "Byzantine": "⚠️" if node.is_byzantine else "✓",
            "View": node.view,
            "Seq#": node.sequence_number,
            "Executed": node.last_executed,
            "State": node.state
        })
    
    df = pd.DataFrame(node_data)
    st.dataframe(df, use_container_width=True, hide_index=True)

with col2:
    st.subheader("📊 Cluster Status")
    
    status = cluster.get_status()
    
    st.metric("Logical Time", f"{status['time']:.0f}")
    st.metric("Current View", cluster.nodes[0].view if cluster.nodes else 0)
    st.metric("Running", "Yes ✅" if status['running'] else "No ⛔")
    
    # Alive nodes
    alive_count = sum(1 for n in cluster.nodes if n.alive)
    st.metric("Alive Nodes", f"{alive_count}/{cluster.num_nodes}")
    
    # Byzantine nodes
    byz_count = sum(1 for n in cluster.nodes if n.is_byzantine)
    st.metric("Byzantine Nodes", f"{byz_count}/{cluster.f} max")
    
    # Pending requests
    st.metric("Pending Requests", len(cluster.pending_requests))
    
    # Show last executed values from primary
    if cluster.nodes:
        primary = cluster.get_primary()
        if primary and primary.app_state:
            st.markdown("**📦 Application State:**")
            for k, v in list(primary.app_state.items())[:5]:
                st.text(f"{k} = {v}")
    
    st.markdown("---")
    
    # Recent events
    st.subheader("📜 Recent Events")
    events = cluster.get_events(100)  # Get more events
    if events:
        # Show last 20 events
        for event in reversed(events[-20:]):
            # Highlight different message types
            if "PRE-PREPARE" in event:
                st.markdown(f"🔵 `{event}`")
            elif "PREPARE" in event:
                st.markdown(f"🟡 `{event}`")
            elif "COMMIT" in event:
                st.markdown(f"🟢 `{event}`")
            elif "EXECUTED" in event:
                st.markdown(f"✅ `{event}`")
            else:
                st.text(event)
    else:
        st.info("👆 Click buttons above to submit requests and see consensus in action!")

# Auto-refresh mechanism when cluster is running
if cluster.is_running():
    current_time = time.time()
    if current_time - st.session_state.last_refresh > 1:  # Refresh every 1 second
        st.session_state.last_refresh = current_time
        st.rerun()

# Additional info section
with st.expander("ℹ️ How pBFT Works"):
    st.markdown("""
    ### Three-Phase Protocol
    
    1. **PRE-PREPARE**: Primary broadcasts request with sequence number
    2. **PREPARE**: Backups validate and broadcast prepare messages
       - Need **2f+1** prepares to be "prepared"
    3. **COMMIT**: After prepared, nodes broadcast commit
       - Need **2f+1** commits to be "committed-local"
       - Then execute the request
    
    ### Key Properties
    
    - **Byzantine Fault Tolerance**: Can handle **f** malicious nodes out of **3f+1** total
    - **Safety**: All correct nodes execute requests in the same order
    - **Liveness**: Requests eventually execute even with faulty primary (view change)
    - **Quorum**: Any two quorums (2f+1 nodes each) overlap in at least **f+1** nodes
    
    ### View Change
    
    - If primary fails or is malicious, backups trigger **view change**
    - New primary is elected: `primary_id = view mod num_nodes`
    - Requires **2f+1** view-change messages to complete
    """)

with st.expander("🧪 Experiment Scenarios"):
    st.markdown("""
    ### Try These Scenarios:
    
    1. **Normal Operation**
       - Start cluster
       - Submit SET/GET operations
       - Observe 3-phase consensus
    
    2. **Primary Crash**
       - Crash the primary node
       - Wait for view change timeout
       - New primary elected, operations continue
    
    3. **Byzantine Node**
       - Mark a backup as Byzantine
       - System still works if ≤ f Byzantine nodes
    
    4. **Multiple Faults**
       - Crash multiple nodes (keep > 2f+1 alive)
       - System maintains consistency
    
    5. **Recovery**
       - Restart crashed nodes
       - Nodes sync with cluster state
    """)
