def state_color(state):
    """Return color for both Raft and pBFT states"""
    return {
        # Raft states
        "LEADER": "#00c853",      # xanh lá
        "CANDIDATE": "#ffab00",   # vàng cam
        "FOLLOWER": "#2962ff",    # xanh dương
        "STOPPED": "#9e9e9e",     # xám
        # pBFT states
        "Normal": "#00c853",      # xanh lá (normal operation)
        "ViewChanging": "#ffab00", # vàng cam (view change)
        "Stopped": "#9e9e9e",     # xám (crashed)
    }.get(state, "#bbbbbb")

def get_node_role_color(node):
    """Get color based on node role (Primary/Backup) and state"""
    if not node.get("alive", True):
        return "#9e9e9e"  # Gray for stopped
    
    if node.get("is_primary", False):
        return "#00c853"  # Green for primary
    elif node.get("is_byzantine", False):
        return "#d32f2f"  # Red for Byzantine
    else:
        return "#2962ff"  # Blue for backup