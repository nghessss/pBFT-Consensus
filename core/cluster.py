# core/cluster_manager.py
import subprocess
import sys

class ClusterManager:
    def __init__(self):
        self.nodes = []        # [{id, port, process}]
        self.running = False

    def is_running(self):
        return self.running

    def resize(self, n):
        if self.running:
            return
        self.nodes = []
        base_port = 5000
        for i in range(n):
            self.nodes.append({
                "id": i + 1,
                "port": base_port + i + 1,
                "process": None
            })

    def start_all(self):
        if self.running:
            return

        peer_args = []
        for node in self.nodes:
            peer_args.append(f"{node['id']}@localhost:{node['port']}")

        peer_str = ",".join(peer_args)

        for node in self.nodes:
            python_cmd = (
                f'{sys.executable} run_node.py '
                f'--id {node["id"]} '
                f'--port {node["port"]} '
                f'--peers "{peer_str}"'
            )

            full_cmd = (
                f'cmd.exe /k '
                f'title RAFT-NODE-{node["id"]} && '
                f'{python_cmd}'
            )

            node["process"] = subprocess.Popen(
                full_cmd,
                creationflags=subprocess.CREATE_NEW_CONSOLE
            )

        self.running = True


    def stop_all(self):
        for node in self.nodes:
            proc = node["process"]
            if proc:
                try:
                    subprocess.run(
                        ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                except Exception as e:
                    print(f"Failed to stop node {node['id']}: {e}")

                node["process"] = None

        self.running = False
