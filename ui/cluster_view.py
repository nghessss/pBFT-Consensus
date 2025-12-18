import math
import os
import sys

from utils.geometry import compute_radius, circle_position
from core.state import state_color

def render_cluster_html(nodes, node_size=70):
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

        color = state_color(node.state)

        html += f"""
        <div style="
            position: absolute;
            top: {y}px;
            left: {x}px;
            width: {node_size}px;
            height: {node_size}px;
            border-radius: 50%;
            background-color: {color};
            border: 2px solid white;
            display: flex;
            justify-content: center;
            align-items: center;
            color: black;
            font-weight: bold;
            font-size: 25px;
            box-shadow: 0 0 10px rgba(0,0,0,0.3);
        ">
            {node.id}
        </div>
        """

    html += "</div>"
    return html
