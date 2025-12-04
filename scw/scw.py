#!/usr/bin/env python3

import subprocess
import json
import time
import sys
from pathlib import Path

# Configuration
ZONE = "fr-par-2"
INSTANCE_TYPE = "H100-1-80G"
IMAGE = "ubuntu_noble_gpu_os_12"
VENV_NAME = "gpu_venv"
JUPYTER_PORT = 8888
STATE_FILE = Path.home() / ".scw_gpu_instance.json"

REMOTE_DIR = "/scratch"

# Work directories
LOCAL_WORK_DIR = Path("./work")
REMOTE_WORK_DIR = "/scratch/work"

# SSH options to disable host key checking
SSH_OPTS = ["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"]

# Remote setup script
REMOTE_SETUP_SCRIPT = """
set -e
apt update -y
apt upgrade -y
apt install -y python3.12-venv jupyter-notebook

cd /scratch
python3 -m venv {venv_name}
source {venv_name}/bin/activate
pip install --upgrade pip
pip install jupyterlab ipykernel
python3 -m ipykernel install --user --name={venv_name} --display-name "Python ({venv_name})"
setsid jupyter notebook --no-browser --port={port} --allow-root --ip=0.0.0.0 --notebook-dir=/scratch > /tmp/jupyter.log 2>&1 < /dev/null &
sleep 5
cat /tmp/jupyter.log
""".format(venv_name=VENV_NAME, port=JUPYTER_PORT)


def run(cmd: list[str], capture: bool = True) -> subprocess.CompletedProcess:
    """Run a command and return result."""
    return subprocess.run(cmd, capture_output=capture, text=True)


def scw(*args) -> subprocess.CompletedProcess:
    """Run a Scaleway CLI command."""
    return run(["scw", *args])


def ssh_run(ip: str, command: str) -> subprocess.CompletedProcess:
    """Run a command on instance via SSH."""
    cmd = ["ssh", *SSH_OPTS, f"root@{ip}", command]
    return run(cmd)


def scp_to_instance(local_path: str, remote_path: str, instance_ip: str, recursive: bool = False) -> subprocess.CompletedProcess:
    """SCP a file or directory to the instance."""
    cmd = ["scp", *SSH_OPTS]
    if recursive:
        cmd.append("-r")
    cmd.extend([local_path, f"root@{instance_ip}:{remote_path}"])
    return run(cmd, capture=False)


def scp_from_instance(remote_path: str, local_path: str, instance_ip: str, recursive: bool = False) -> subprocess.CompletedProcess:
    """SCP a file or directory from the instance."""
    cmd = ["scp", *SSH_OPTS]
    if recursive:
        cmd.append("-r")
    cmd.extend([f"root@{instance_ip}:{remote_path}", local_path])
    return run(cmd, capture=False)


def remove_from_known_hosts(ip: str) -> None:
    """Remove IP from known_hosts to avoid host key conflicts."""
    known_hosts = Path.home() / ".ssh" / "known_hosts"
    if known_hosts.exists():
        run(["ssh-keygen", "-R", ip])
        print(f"Removed {ip} from known_hosts")


def save_state(instance_id: str, instance_ip: str) -> None:
    """Save instance state to file."""
    STATE_FILE.write_text(json.dumps({"id": instance_id, "ip": instance_ip}))


def load_state() -> dict | None:
    """Load instance state from file."""
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return None


def delete_state() -> None:
    """Delete state file."""
    STATE_FILE.unlink(missing_ok=True)


def get_instance_state(instance_id: str) -> str:
    """Get instance state."""
    result = scw("instance", "server", "get", instance_id, f"zone={ZONE}", "-o", "json")
    data = json.loads(result.stdout)
    return data.get("state", "")


def get_instance_ip(instance_id: str) -> str | None:
    """Get public IP for an instance from IP list."""
    result = scw("instance", "ip", "list", f"zone={ZONE}", "-o", "json")
    if result.returncode != 0:
        return None

    ips = json.loads(result.stdout)
    for ip_info in ips:
        server = ip_info.get("server")
        if server and server.get("id") == instance_id:
            return ip_info.get("address")
    return None


def wait_for_running(instance_id: str) -> None:
    """Wait for instance to reach running state."""
    print("Waiting for instance to start", end="", flush=True)
    while True:
        state = get_instance_state(instance_id)
        if state == "running":
            print(" Ready!")
            return
        print(".", end="", flush=True)
        time.sleep(5)


def wait_for_ip(instance_id: str) -> str:
    """Wait for public IP to be assigned and return it."""
    print("Waiting for IP assignment", end="", flush=True)
    while True:
        ip = get_instance_ip(instance_id)
        if ip:
            print(f" {ip}")
            return ip
        print(".", end="", flush=True)
        time.sleep(5)


def wait_for_ssh(instance_ip: str) -> None:
    """Wait for SSH to become available."""
    print("Waiting for SSH", end="", flush=True)
    while True:
        result = ssh_run(instance_ip, "echo ok")
        if result.returncode == 0:
            print(" Ready!")
            return
        print(".", end="", flush=True)
        time.sleep(5)


def extract_jupyter_token(log_output: str) -> str:
    """Extract Jupyter token from log output."""
    import re
    match = re.search(r'token=([a-zA-Z0-9_-]+)', log_output)
    return match.group(1) if match else ""


def start_ssh_tunnel(instance_ip: str) -> None:
    """Start SSH tunnel in foreground (blocking)."""
    print(f"\nStarting SSH tunnel to {instance_ip}...")
    print("Press Ctrl+C to disconnect, then run './scw.py stop' to terminate instance.\n")
    cmd = ["ssh", "-N", "-L", f"{JUPYTER_PORT}:localhost:{JUPYTER_PORT}", *SSH_OPTS, f"root@{instance_ip}"]
    try:
        subprocess.run(cmd)
    except KeyboardInterrupt:
        print("\nTunnel closed.")


def upload_work_dir(instance_ip: str) -> None:
    """Upload entire ./work directory to instance."""
    if not LOCAL_WORK_DIR.exists():
        print(f"Local work directory {LOCAL_WORK_DIR} does not exist, skipping upload.")
        return

    print(f"Uploading {LOCAL_WORK_DIR} to {REMOTE_WORK_DIR}...")
    # Ensure remote directory exists
    ssh_run(instance_ip, f"mkdir -p {REMOTE_WORK_DIR}")

    # Upload contents (use trailing slash to copy contents, not the dir itself)
    result = scp_to_instance(f"{LOCAL_WORK_DIR}/.", REMOTE_WORK_DIR, instance_ip, recursive=True)
    if result.returncode != 0:
        print(f"Warning: Failed to upload work directory: {result.stderr}")
    else:
        print("Work directory uploaded successfully.")


def download_work_dir(instance_ip: str) -> None:
    """Download entire /scratch/work directory from instance."""
    LOCAL_WORK_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Downloading {REMOTE_WORK_DIR} to {LOCAL_WORK_DIR}...")

    # Check if remote directory exists
    check = ssh_run(instance_ip, f"test -d {REMOTE_WORK_DIR} && echo exists")
    if "exists" not in check.stdout:
        print(f"Remote work directory {REMOTE_WORK_DIR} does not exist, skipping download.")
        return

    # Download contents
    result = scp_from_instance(f"{REMOTE_WORK_DIR}/.", str(LOCAL_WORK_DIR), instance_ip, recursive=True)
    if result.returncode != 0:
        print(f"Warning: Failed to download work directory: {result.stderr}")
    else:
        print("Work directory downloaded successfully.")


def upload(filename: str) -> None:
    """Upload a single file from ./work to /scratch/work on the instance."""
    state = load_state()
    if not state:
        print("No running instance found. Run 'start' first.")
        sys.exit(1)

    instance_ip = state["ip"]
    local_path = LOCAL_WORK_DIR / filename
    remote_path = f"{REMOTE_WORK_DIR}/{filename}"

    if not local_path.exists():
        print(f"Error: {local_path} does not exist.")
        sys.exit(1)

    # Ensure remote directory exists
    ssh_run(instance_ip, f"mkdir -p {REMOTE_WORK_DIR}")

    is_dir = local_path.is_dir()
    print(f"Uploading {local_path} to {remote_path}...")
    result = scp_to_instance(str(local_path), remote_path, instance_ip, recursive=is_dir)

    if result.returncode != 0:
        print(f"Upload failed: {result.stderr}")
        sys.exit(1)
    print("Upload complete.")


def download(filename: str) -> None:
    """Download a single file from /scratch/work to ./work from the instance."""
    state = load_state()
    if not state:
        print("No running instance found. Run 'start' first.")
        sys.exit(1)

    instance_ip = state["ip"]
    remote_path = f"{REMOTE_WORK_DIR}/{filename}"
    local_path = LOCAL_WORK_DIR / filename

    # Ensure local directory exists
    LOCAL_WORK_DIR.mkdir(parents=True, exist_ok=True)

    # Check if remote path is a directory
    check = ssh_run(instance_ip, f"test -d {remote_path} && echo isdir")
    is_dir = "isdir" in check.stdout

    print(f"Downloading {remote_path} to {local_path}...")
    result = scp_from_instance(remote_path, str(local_path), instance_ip, recursive=is_dir)

    if result.returncode != 0:
        print(f"Download failed: {result.stderr}")
        sys.exit(1)
    print("Download complete.")


def start() -> None:
    """Create instance, set it up, and start Jupyter."""
    # Check if already running
    state = load_state()
    if state:
        print(f"Instance already exists: {state['id']} ({state['ip']})")
        print("Run 'stop' first to terminate it.")
        sys.exit(1)

    # Create instance
    print(f"Creating {INSTANCE_TYPE} instance...")
    result = scw("instance", "server", "create", f"type={INSTANCE_TYPE}",
                 f"zone={ZONE}", f"image={IMAGE}", "-o", "json")

    if result.returncode != 0:
        print(f"Failed to create instance: {result.stderr}")
        sys.exit(1)

    data = json.loads(result.stdout)
    instance_id = data["id"]
    print(f"Instance ID: {instance_id}")

    # Wait for instance to be running
    wait_for_running(instance_id)

    # Wait for IP to be assigned
    instance_ip = wait_for_ip(instance_id)

    # Remove old host key for this IP
    remove_from_known_hosts(instance_ip)

    # Save state
    save_state(instance_id, instance_ip)

    # Wait for SSH
    wait_for_ssh(instance_ip)

    # Upload work directory
    upload_work_dir(instance_ip)

    # Run setup script
    print("Setting up instance (apt upgrade, venv, jupyter)...")
    result = ssh_run(instance_ip, REMOTE_SETUP_SCRIPT)

    if result.returncode != 0:
        print(f"Setup failed: {result.stderr}")
        sys.exit(1)

    # Extract token
    token = extract_jupyter_token(result.stdout)

    # Print connection info
    print("\n" + "=" * 60)
    print("SETUP COMPLETE")
    print("=" * 60)
    print(f"\nInstance ID: {instance_id}")
    print(f"Instance IP: {instance_ip}")
    print(f"\nOpen in browser:")
    if token:
        print(f"  http://localhost:{JUPYTER_PORT}/?token={token}")
    else:
        print(f"  http://localhost:{JUPYTER_PORT}")
    print("=" * 60)

    # Start SSH tunnel (blocking)
    start_ssh_tunnel(instance_ip)


def stop() -> None:
    """Stop and delete the instance."""
    state = load_state()
    if not state:
        print("No instance found. Nothing to stop.")
        sys.exit(0)

    instance_id = state["id"]
    instance_ip = state.get("ip")

    # Download work directory before stopping
    if instance_ip:
        download_work_dir(instance_ip)

    print(f"Stopping and deleting instance {instance_id}...")

    # Remove from known_hosts
    if instance_ip:
        remove_from_known_hosts(instance_ip)

    # Stop instance
    print("Stopping instance...")
    result = scw("instance", "server", "stop", instance_id, f"zone={ZONE}", "--wait")
    if result.returncode != 0:
        print(f"Warning: Stop may have failed: {result.stderr}")

    # Delete instance with volumes and IP
    print("Deleting instance...")
    result = scw("instance", "server", "delete", instance_id, f"zone={ZONE}",
                 "with-ip=true", "with-volumes=local")

    if result.returncode != 0:
        print(f"Warning: Delete may have failed: {result.stderr}")

    delete_state()
    print("Instance deleted. State cleared.")


def main() -> None:
    """Main entry point."""
    if len(sys.argv) < 2 or sys.argv[1] not in ("start", "stop", "upload", "download"):
        print("Usage: ./scw.py <command> [args]")
        print()
        print("Commands:")
        print("  start            Create instance, upload ./work, install deps, start Jupyter, open tunnel")
        print("  stop             Download /scratch/work, stop and delete instance (full cleanup)")
        print("  upload <file>    Upload file from ./work/<file> to /scratch/work/<file>")
        print("  download <file>  Download file from /scratch/work/<file> to ./work/<file>")
        sys.exit(1)

    command = sys.argv[1]

    if command == "start":
        start()
    elif command == "stop":
        stop()
    elif command == "upload":
        if len(sys.argv) != 3:
            print("Usage: ./scw.py upload <filename>")
            sys.exit(1)
        upload(sys.argv[2])
    elif command == "download":
        if len(sys.argv) != 3:
            print("Usage: ./scw.py download <filename>")
            sys.exit(1)
        download(sys.argv[2])


if __name__ == "__main__":
    main()
