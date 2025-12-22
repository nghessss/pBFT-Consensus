import math
import os
import sys

from ui.utils.geometry import compute_radius, circle_position
from core.state import state_color, get_node_role_color

def render_cluster_html(nodes, node_size=70, use_pbft=True):
    """Render cluster visualization for both Raft and pBFT"""
    n = len(nodes)
    radius = compute_radius(n, node_size)
    container_size = int(2 * radius + node_size + 40)

    html = f"""
    <div style="
        width: {container_size}px;
        height: {container_size}px;
        position: relative;
        margin: auto;
        margin-top: 30px;
    ">
    """

    for i, node in enumerate(nodes):
        x, y = circle_position(container_size, radius, i, n)
        x -= node_size / 2
        y -= node_size / 2

        # Get node info (handle both dict and object)
        if isinstance(node, dict):
            node_id = node.get('id', i)
            is_primary = node.get('is_primary', False)
            is_byzantine = node.get('is_byzantine', False)
            alive = node.get('alive', True)
            view = node.get('view', 0)
            color = get_node_role_color(node) if use_pbft else state_color(node.get('state', 'Normal'))
        else:
            node_id = getattr(node, 'id', i)
            is_primary = getattr(node, 'is_primary', lambda: False)()
            is_byzantine = getattr(node, 'is_byzantine', False)
            alive = getattr(node, 'alive', True)
            view = getattr(node, 'view', 0)
            color = get_node_role_color({'is_primary': is_primary, 'is_byzantine': is_byzantine, 'alive': alive}) if use_pbft else state_color(getattr(node, 'state', 'Normal'))

        # Add crown icon for primary
        crown = "👑" if is_primary and alive else ""
        # Add warning icon for Byzantine
        warning = "⚠️" if is_byzantine else ""
        
        # Border style
        border_style = "3px solid #ffd700" if is_primary else "2px solid white"
        if is_byzantine:
            border_style = "3px dashed #d32f2f"

        html += f"""
        <div style="
            position: absolute;
            top: {y}px;
            left: {x}px;
            width: {node_size}px;
            height: {node_size}px;
            border-radius: 50%;
            background-color: {color};
            border: {border_style};
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            color: white;
            font-weight: bold;
            font-size: 24px;
            box-shadow: 0 0 15px rgba(0,0,0,0.4);
            opacity: {0.4 if not alive else 1.0};
        ">
            <div style="font-size: 14px; position: absolute; top: 2px;">{crown}{warning}</div>
            <div>{node_id}</div>
            <div style="font-size: 10px; position: absolute; bottom: 5px;">v{view}</div>
        </div>
        """

    html += "</div>"
    return html
