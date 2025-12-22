import streamlit as st

def render_sidebar(cluster):
    """Render sidebar controls for pBFT cluster"""
    st.sidebar.header("⚙️ pBFT Cluster Controls")

    # Show cluster info
    st.sidebar.markdown(f"**Total Nodes:** {cluster.num_nodes}")
    st.sidebar.markdown(f"**Max Faulty (f):** {cluster.f}")
    st.sidebar.markdown(f"**Quorum (2f+1):** {2 * cluster.f + 1}")
    
    primary = cluster.get_primary()
    if primary:
        st.sidebar.markdown(f"**Primary:** Node {primary.id} (View {primary.view})")
    else:
        st.sidebar.markdown("**Primary:** None")
    
    st.sidebar.markdown("---")

    # Node count adjustment
    node_count = st.sidebar.number_input(
        "Number of nodes (3f+1)",
        min_value=4,
        max_value=13,
        value=len(cluster.nodes),
        step=3,  # Increment by 3 to maintain 3f+1
        help="Total nodes must be 3f+1 where f is max faulty nodes"
    )

    if not cluster.is_running():
        if st.sidebar.button("Apply Node Count", use_container_width=True):
            cluster.resize(node_count)
            st.sidebar.success(f"Cluster resized to {node_count} nodes (f={cluster.f})")
            st.rerun()
    else:
        st.sidebar.info("Stop cluster before resizing.")

    # Start/Stop cluster
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("▶️ Start", use_container_width=True):
            cluster.start_all()
    with col2:
        if st.button("⛔ Stop", use_container_width=True):
            cluster.stop_all()

    st.sidebar.markdown("---")
    
    # Client operations
    st.sidebar.markdown("### 📝 Client Operations")
    
    op_type = st.sidebar.selectbox("Operation Type", ["SET", "GET", "DELETE"])
    
    if op_type == "SET":
        key = st.sidebar.text_input("Key", "x")
        value = st.sidebar.text_input("Value", "100")
        if st.sidebar.button("Submit SET", use_container_width=True):
            cluster.submit_request({"type": "SET", "key": key, "value": value}, client_id=1)
            st.sidebar.success(f"SET {key}={value} submitted")
    
    elif op_type == "GET":
        key = st.sidebar.text_input("Key", "x")
        if st.sidebar.button("Submit GET", use_container_width=True):
            cluster.submit_request({"type": "GET", "key": key}, client_id=1)
            st.sidebar.success(f"GET {key} submitted")
    
    elif op_type == "DELETE":
        key = st.sidebar.text_input("Key", "x")
        if st.sidebar.button("Submit DELETE", use_container_width=True):
            cluster.submit_request({"type": "DELETE", "key": key}, client_id=1)
            st.sidebar.success(f"DELETE {key} submitted")

    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔍 Node Controls")

    for node in cluster.nodes:
        role = "Primary" if node.is_primary() else "Backup"
        status = "🔴" if not node.alive else ("⚠️" if node.is_byzantine else "🟢")
        
        with st.sidebar.expander(f"{status} Node {node.id} ({role})"):
            st.write(f"**View:** {node.view}")
            st.write(f"**State:** {node.state}")
            st.write(f"**Sequence:** {node.sequence_number}")
            st.write(f"**Executed:** {node.last_executed}")
            st.write(f"**Checkpoint:** {node.stable_checkpoint}")
            st.write(f"**Alive:** {node.alive}")
            st.write(f"**Byzantine:** {node.is_byzantine}")

            c1, c2 = st.columns(2)
            with c1:
                if not node.alive:
                    if st.button("🔄 Restart", key=f"start_{node.id}", use_container_width=True):
                        cluster.restart_node(node.id)
                else:
                    if st.button("💥 Crash", key=f"crash_{node.id}", use_container_width=True):
                        cluster.crash_node(node.id)
            
            with c2:
                if node.is_byzantine:
                    if st.button("✅ Fix", key=f"fix_{node.id}", use_container_width=True):
                        cluster.set_byzantine(node.id, False)
                else:
                    if st.button("⚠️ Byzantine", key=f"byz_{node.id}", use_container_width=True):
                        cluster.set_byzantine(node.id, True)
