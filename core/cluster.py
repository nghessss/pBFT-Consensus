# core/cluster_manager.py
import subprocess
import sys

class ClusterManager:
    def __init__(self):
        self.nodes = []        # [{id, port, process}]
        self.running = False
        self.last_error = ""

    def is_running(self):
        return self.running

    def validate_node_count(self, n: int) -> tuple[bool, int]:
        # PBFT classic requirement: n = 3f + 1
        if n <= 0:
            return False, 0
        f = (n - 1) // 3
        ok = (3 * f + 1) == n
        return ok, f

    def resize(self, n):
        if self.running:
            return

        ok, f = self.validate_node_count(int(n))
        if not ok:
            self.last_error = f"Invalid PBFT node count n={n}. Must be n = 3f + 1 (e.g. 4, 7, 10)."
            self.nodes = []
            return

        self.last_error = ""
        self.nodes = []
        base_port = 5000
        for i in range(n):
            self.nodes.append({
                "id": i + 1,
                "port": base_port + i + 1,
                "process": None,
                "byzantine": False,
            })

    def _peer_str(self) -> str:
        peer_args = []
        for node in self.nodes:
            peer_args.append(f"{node['id']}@localhost:{node['port']}")
        return ",".join(peer_args)

    def _build_python_cmd(self, node: dict) -> str:
        byz_arg = " --byzantine" if node.get("byzantine") else ""
        python_cmd = (
            f'{sys.executable} run_node.py '
            f'--id {node["id"]} '
            f'--port {node["port"]} '
            f'--peers "{self._peer_str()}"'
            f'{byz_arg}'
        )
        return python_cmd

    def start_node(self, node_id: int):
        node = next((n for n in self.nodes if int(n.get("id")) == int(node_id)), None)
        if node is None:
            return
        if node.get("process"):
            return

        python_cmd = self._build_python_cmd(node)
        full_cmd = f'cmd.exe /k title PBFT-Node-{node["id"]} && {python_cmd}'
        node["process"] = subprocess.Popen(
            full_cmd,
            creationflags=subprocess.CREATE_NEW_CONSOLE
        )

        # Update global running flag
        self.running = any(n.get("process") for n in self.nodes)

    def stop_node(self, node_id: int):
        node = next((n for n in self.nodes if int(n.get("id")) == int(node_id)), None)
        if node is None:
            return

        proc = node.get("process")
        if not proc:
            return

        try:
            subprocess.run(
                ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
        except Exception as e:
            print(f"Failed to stop node {node['id']}: {e}")

        node["process"] = None
        self.running = any(n.get("process") for n in self.nodes)

    def start_all(self):
        if self.running:
            return

        ok, _ = self.validate_node_count(len(self.nodes))
        if not ok:
            self.last_error = (
                f"Invalid PBFT node count n={len(self.nodes)}. Must be n = 3f + 1 (e.g. 4, 7, 10)."
            )
            return

        self.last_error = ""

        for node in self.nodes:
            self.start_node(node["id"])


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
