import streamlit as st

def render_sidebar(cluster):
    st.sidebar.header("⚙️ Cluster Controls")

    node_count = st.sidebar.number_input(
        "Number of nodes",
        min_value=3,
        max_value=10,
        value=len(cluster.nodes),
        step=1
    )

    if not cluster.is_running():
        if st.sidebar.button("Apply Node Count", use_container_width=True):
            cluster.resize(node_count)
            st.sidebar.success(f"Cluster resized to {node_count} nodes")
    else:
        st.sidebar.info("Stop cluster before resizing.")

    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("▶️ Start all", use_container_width=True):
            cluster.start_all()
    with col2:
        if st.button("⛔ Stop all", use_container_width=True):
            cluster.stop_all()

    st.sidebar.markdown("### 🔍 Node Details")

    for node in cluster.nodes:
        with st.sidebar.expander(f"Node {node.id} - {node.state}"):

            st.write(f"**State:** {node.state}")
            st.write(f"**Term:** {node.current_term}")
            st.write(f"**Alive:** {node.alive}")
            st.write(f"**Election timeout:** {node.election_timeout}")

            c1, c2 = st.columns(2)
            with c1:
                if st.button(f"Start Node", key=f"start_{node.id}", use_container_width=True):
                    node.resume(now_time=cluster.current_time)
            with c2:
                if st.button(f"Stop Node", key=f"stop_{node.id}", use_container_width=True):
                    node.stop()
