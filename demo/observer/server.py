#!/usr/bin/env python3
"""Observer for the resec quorum-loss demo.

Serves an HTML page with the status of Consul, Redis and resec, and a live
log flow of all containers in the same compose project.

Uses only the Python standard library. Needs /var/run/docker.sock.
"""

import collections
import concurrent.futures
import datetime
import http.client
import http.server
import json
import os
import socket
import threading
import time
import urllib.parse

DOCKER_SOCK = os.environ.get("DOCKER_SOCK", "/var/run/docker.sock")
LISTEN = int(os.environ.get("OBSERVER_PORT", "8090"))
LOCK_KEY = os.environ.get("CONSUL_LOCK_KEY", "resec/.lock")
SERVICE_PREFIX = os.environ.get("CONSUL_SERVICE_PREFIX", "redis")
HERE = os.path.dirname(os.path.abspath(__file__))
TIMEOUT = 0.8


# ---------------------------------------------------------------- docker API

class UnixConn(http.client.HTTPConnection):
    def __init__(self, timeout=None):
        super().__init__("localhost", timeout=timeout)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        if self.timeout:
            self.sock.settimeout(self.timeout)
        self.sock.connect(DOCKER_SOCK)


def docker_get(path, timeout=5):
    conn = UnixConn(timeout=timeout)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        body = resp.read()
        if resp.status >= 300:
            raise RuntimeError(f"docker {path}: {resp.status} {body[:200]!r}")
        return json.loads(body)
    finally:
        conn.close()


def docker_stream(path):
    conn = UnixConn(timeout=None)
    conn.request("GET", path)
    resp = conn.getresponse()
    if resp.status >= 300:
        conn.close()
        raise RuntimeError(f"docker {path}: {resp.status}")
    return conn, resp


def find_project():
    """Compose project of this container (or env COMPOSE_PROJECT)."""
    if os.environ.get("COMPOSE_PROJECT"):
        return os.environ["COMPOSE_PROJECT"]
    info = docker_get(f"/containers/{socket.gethostname()}/json")
    return info["Config"]["Labels"]["com.docker.compose.project"]


PROJECT = None
SELF_SERVICE = None


def list_containers():
    flt = json.dumps({"label": [f"com.docker.compose.project={PROJECT}"]})
    items = docker_get("/containers/json?all=1&filters=" + urllib.parse.quote(flt))
    out = []
    for c in items:
        svc = c["Labels"].get("com.docker.compose.service", c["Names"][0].lstrip("/"))
        if svc == SELF_SERVICE:
            continue
        out.append({
            "id": c["Id"],
            "service": svc,
            "state": c["State"],
            "status": c["Status"],
        })
    out.sort(key=lambda c: c["service"])
    return out


# ---------------------------------------------------------------- log buffer

class LogBuffer:
    def __init__(self, size=5000):
        self.items = collections.deque(maxlen=size)
        self.seq = 0
        self.cond = threading.Condition()

    def add(self, service, kind, line, ts=None):
        with self.cond:
            self.seq += 1
            self.items.append({
                "seq": self.seq,
                "ts": ts or now_iso(),
                "svc": service,
                "kind": kind,  # stdout, stderr, event, observer
                "line": line,
            })
            self.cond.notify_all()

    def since(self, seq):
        with self.cond:
            return [i for i in self.items if i["seq"] > seq]

    def wait(self, seq, timeout):
        with self.cond:
            if self.seq <= seq:
                self.cond.wait(timeout)
            return [i for i in self.items if i["seq"] > seq]


LOGS = LogBuffer()


def now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def ts_key(ts):
    """RFC3339Nano -> float seconds (for de-duplication)."""
    try:
        base, frac = ts.rstrip("Z").split(".") if "." in ts else (ts.rstrip("Z"), "0")
        dt = datetime.datetime.strptime(base, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc)
        return dt.timestamp() + float("0." + frac)
    except Exception:
        return 0.0


class Follower(threading.Thread):
    """Follows the logs of one container. Ends when the container stops."""

    def __init__(self, cid, service, since):
        super().__init__(daemon=True)
        self.cid, self.service, self.since = cid, service, since
        self.last = since

    def run(self):
        q = "follow=1&stdout=1&stderr=1&timestamps=1"
        q += f"&since={int(self.since)}" if self.since else "&tail=200"
        try:
            conn, resp = docker_stream(f"/containers/{self.cid}/logs?{q}")
        except Exception as e:
            LOGS.add(self.service, "observer", f"cannot follow logs: {e}")
            return
        try:
            pending = {1: b"", 2: b""}
            while True:
                hdr = resp.read(8)
                if len(hdr) < 8:
                    break
                if hdr[0] not in (0, 1, 2) or hdr[1:4] != b"\0\0\0":
                    # TTY container: raw stream
                    rest = hdr + (resp.readline() or b"")
                    self.emit(1, rest.decode("utf-8", "replace"))
                    continue
                size = int.from_bytes(hdr[4:8], "big")
                data = resp.read(size)
                stream = 2 if hdr[0] == 2 else 1
                buf = pending[stream] + data
                *lines, pending[stream] = buf.split(b"\n")
                for ln in lines:
                    self.emit(stream, ln.decode("utf-8", "replace"))
        except Exception:
            pass
        finally:
            conn.close()

    def emit(self, stream, raw):
        raw = raw.rstrip("\r")
        ts, _, line = raw.partition(" ")
        key = ts_key(ts)
        if key and key <= self.last:
            return  # already seen before re-attach
        self.last = max(self.last, key)
        LAST_SEEN[self.cid] = self.last
        LOGS.add(self.service, "stderr" if stream == 2 else "stdout", line,
                 ts=ts[:23] + "Z" if key else None)


FOLLOWERS = {}
LAST_SEEN = {}


def log_supervisor():
    """Starts a follower for each running container, re-attaches after restart."""
    while True:
        try:
            for c in list_containers():
                f = FOLLOWERS.get(c["id"])
                if c["state"] == "running" and (f is None or not f.is_alive()):
                    f = Follower(c["id"], c["service"], LAST_SEEN.get(c["id"], 0))
                    FOLLOWERS[c["id"]] = f
                    f.start()
        except Exception as e:
            LOGS.add("observer", "observer", f"log supervisor: {e}")
        time.sleep(1)


def event_watcher():
    """Puts docker container events (start, stop, die, ...) into the log flow."""
    interesting = {"start", "stop", "die", "kill", "restart", "pause", "unpause", "oom"}
    while True:
        try:
            flt = json.dumps({"type": ["container"], "label": [f"com.docker.compose.project={PROJECT}"]})
            conn, resp = docker_stream("/events?filters=" + urllib.parse.quote(flt))
            while True:
                line = resp.readline()
                if not line:
                    break
                ev = json.loads(line)
                action = ev.get("Action", ev.get("status", ""))
                if action not in interesting:
                    continue
                attrs = ev.get("Actor", {}).get("Attributes", {})
                svc = attrs.get("com.docker.compose.service", attrs.get("name", "?"))
                if svc == SELF_SERVICE:
                    continue
                extra = f" (exit {attrs['exitCode']})" if action == "die" and "exitCode" in attrs else ""
                LOGS.add(svc, "event", f"container {action}{extra}")
        except Exception as e:
            LOGS.add("observer", "observer", f"event watcher: {e}")
        time.sleep(2)


# ---------------------------------------------------------------- probes

def http_get(host, port, path, timeout=TIMEOUT):
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        return resp.status, resp.read().decode("utf-8", "replace")
    finally:
        conn.close()


def redis_info(host, port=6379, timeout=TIMEOUT):
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.settimeout(timeout)
        s.sendall(b"*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n")
        f = s.makefile("rb")
        head = f.readline()
        if not head.startswith(b"$"):
            raise RuntimeError(head.decode(errors="replace").strip())
        body = f.read(int(head[1:]))
    info = {}
    for ln in body.decode().split("\r\n"):
        if ":" in ln and not ln.startswith("#"):
            k, v = ln.split(":", 1)
            info[k] = v
    return info


def probe_redis(svc):
    try:
        info = redis_info(svc)
        return {
            "ok": True,
            "role": info.get("role"),
            "master_host": info.get("master_host"),
            "master_port": info.get("master_port"),
            "master_link_status": info.get("master_link_status"),
            "master_link_down_since_seconds": info.get("master_link_down_since_seconds"),
            "connected_slaves": info.get("connected_slaves"),
            "offset": info.get("master_repl_offset"),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def probe_resec(svc):
    out = {}
    try:
        code, body = http_get(svc, 8080, "/health", timeout=3)
        out["health_code"], out["health"] = code, body.strip()
    except Exception as e:
        out["health_code"], out["health"] = 0, str(e)
    try:
        code, body = http_get(svc, 8080, "/state")
        st = json.loads(body)
        cs, rs = st.get("consulState", {}), st.get("redisState", {})
        out["consul"] = {k: cs.get(k) for k in ("Ready", "Healthy", "Master", "MasterAddr", "MasterPort", "MasterServices")}
        out["redis"] = {
            "Ready": rs.get("Ready"),
            "Healthy": rs.get("Healthy"),
            "Role": (rs.get("Info") or {}).get("Role"),
        }
    except Exception as e:
        out["state_error"] = str(e)
    return out


def probe_consul_server(svc):
    out = {}
    try:
        code, body = http_get(svc, 8500, "/v1/status/leader")
        out["leader"] = json.loads(body) if code == 200 else ""
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        code, body = http_get(svc, 8500, "/v1/agent/self")
        cfg = json.loads(body)
        out["node"] = cfg["Config"]["NodeName"]
        out["addr"] = cfg["Member"]["Addr"]
        out["raft_state"] = (cfg.get("Stats", {}).get("raft", {}) or {}).get("state")
    except Exception:
        pass
    out["ok"] = True
    return out


def probe_consul_agent(svc):
    try:
        code, body = http_get(svc, 8500, "/v1/agent/self")
        cfg = json.loads(body)
        out = {"ok": True, "node": cfg["Config"]["NodeName"]}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        code, body = http_get(svc, 8500, "/v1/agent/services")
        out["services"] = [
            {"id": s["ID"], "name": s["Service"], "port": s["Port"], "address": s["Address"]}
            for s in json.loads(body).values()
        ]
        code, body = http_get(svc, 8500, "/v1/agent/checks")
        out["checks"] = [
            {"id": c["CheckID"], "status": c["Status"]} for c in json.loads(body).values()
        ]
    except Exception:
        pass
    return out


def probe_cluster(agents):
    """Cluster view through any reachable agent."""
    out = {"quorum": False, "quorum_error": None, "lock": None, "catalog": {}}
    for a in agents:
        try:
            # agent reachable?
            http_get(a, 8500, "/v1/agent/self")
        except Exception as e:
            out["quorum_error"] = f"{a}: {e}"
            continue
        out["via"] = a
        try:
            code, body = http_get(a, 8500, f"/v1/kv/{LOCK_KEY}?consistent", timeout=1.5)
            if code in (200, 404):
                out["quorum"] = True
            else:
                out["quorum_error"] = f"{code} {body.strip()}"
        except Exception as e:
            out["quorum_error"] = f"consistent read: {e}"
        # stale reads work without a leader
        try:
            code, body = http_get(a, 8500, f"/v1/kv/{LOCK_KEY}?stale")
            if code == 200:
                kv = json.loads(body)[0]
                lock = {"session": kv.get("Session"), "index": kv.get("ModifyIndex")}
                if kv.get("Session"):
                    code, body = http_get(a, 8500, f"/v1/session/info/{kv['Session']}?stale")
                    sess = json.loads(body) if code == 200 else None
                    if sess:
                        lock["node"] = sess[0].get("Node")
                        lock["name"] = sess[0].get("Name")
                out["lock"] = lock
            else:
                out["lock"] = {"missing": True}
            for role in ("master", "slave"):
                code, body = http_get(a, 8500, f"/v1/health/service/{SERVICE_PREFIX}-{role}?stale")
                entries = json.loads(body) if code == 200 else []
                out["catalog"][role] = [{
                    "node": e["Node"]["Node"],
                    "address": e["Service"]["Address"] or e["Node"]["Address"],
                    "port": e["Service"]["Port"],
                    "status": worst([c["Status"] for c in e["Checks"]]),
                } for e in entries]
        except Exception as e:
            out["catalog_error"] = str(e)
        break
    return out


def worst(statuses):
    for s in ("critical", "warning", "passing"):
        if s in statuses:
            return s
    return "unknown"


STATUS = {"ts": None}
STATUS_LOCK = threading.Lock()


def status_poller():
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=16)
    prev = {}
    while True:
        started = time.time()
        try:
            containers = list_containers()
            running = {c["service"] for c in containers if c["state"] == "running"}
            groups = {"consul_server": [], "consul_agent": [], "redis": [], "resec": []}
            for c in containers:
                s = c["service"]
                if s.startswith("consul-server"):
                    groups["consul_server"].append(s)
                elif s.startswith("consul"):
                    groups["consul_agent"].append(s)
                elif s.startswith("redis"):
                    groups["redis"].append(s)
                elif s.startswith("resec"):
                    groups["resec"].append(s)

            probes = {"consul_server": probe_consul_server, "consul_agent": probe_consul_agent,
                      "redis": probe_redis, "resec": probe_resec}
            futs = {}
            for g, names in groups.items():
                for n in names:
                    if n in running:
                        futs[(g, n)] = pool.submit(probes[g], n)
            agents = [a for a in groups["consul_agent"] + groups["consul_server"] if a in running]
            cluster_f = pool.submit(probe_cluster, agents)

            result = {g: {} for g in groups}
            for (g, n), f in futs.items():
                try:
                    result[g][n] = f.result(timeout=5)
                except Exception as e:
                    result[g][n] = {"ok": False, "error": str(e)}
            status = {
                "ts": now_iso(),
                "project": PROJECT,
                "containers": containers,
                "groups": groups,
                "probes": result,
                "cluster": cluster_f.result(timeout=5),
            }
            note_changes(prev, status)
            prev = status
            with STATUS_LOCK:
                STATUS.clear()
                STATUS.update(status)
        except Exception as e:
            LOGS.add("observer", "observer", f"status poller: {e}")
        time.sleep(max(0.2, 1.0 - (time.time() - started)))


def note_changes(prev, cur):
    """Put important state changes into the log flow."""
    if not prev:
        return
    pc, cc = prev.get("cluster", {}), cur.get("cluster", {})
    if pc.get("quorum") != cc.get("quorum"):
        LOGS.add("observer", "change", "Consul quorum " + ("OK" if cc.get("quorum") else f"LOST ({cc.get('quorum_error')})"))
    pl, cl = (pc.get("lock") or {}).get("node"), (cc.get("lock") or {}).get("node")
    if pl != cl:
        LOGS.add("observer", "change", f"lock holder: {pl or '-'} -> {cl or '-'}")
    for n, r in cur["probes"].get("redis", {}).items():
        o = prev["probes"].get("redis", {}).get(n, {})
        if (o.get("role"), o.get("master_host")) != (r.get("role"), r.get("master_host")):
            tgt = f" of {r.get('master_host')}:{r.get('master_port')}" if r.get("role") == "slave" else ""
            LOGS.add(n, "change", f"redis role: {r.get('role') or 'down'}{tgt}")
    for n, r in cur["probes"].get("resec", {}).items():
        o = prev["probes"].get("resec", {}).get(n, {})
        if o.get("health_code") != r.get("health_code"):
            LOGS.add(n, "change", f"/health {r.get('health_code')}: {r.get('health')}")


# ---------------------------------------------------------------- HTTP

class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def send(self, code, ctype, body):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        if url.path in ("/", "/index.html"):
            with open(os.path.join(HERE, "index.html"), "rb") as f:
                self.send(200, "text/html; charset=utf-8", f.read())
        elif url.path == "/api/status":
            with STATUS_LOCK:
                body = json.dumps(STATUS).encode()
            self.send(200, "application/json", body)
        elif url.path == "/api/results":
            # written by demo/scenario.sh into the mounted observer directory
            try:
                with open(os.path.join(HERE, "results.json"), "rb") as f:
                    body = f.read()
            except OSError:
                body = b'{"scenarios": []}'
            self.send(200, "application/json", body)
        elif url.path == "/api/logs":
            self.stream_logs(url)
        else:
            self.send(404, "text/plain", b"not found")

    def stream_logs(self, url):
        qs = urllib.parse.parse_qs(url.query)
        seq = int(qs.get("since", ["0"])[0])
        last_id = self.headers.get("Last-Event-ID")
        if last_id and last_id.isdigit():
            seq = int(last_id)
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        try:
            items = LOGS.since(seq)
            while True:
                if items:
                    seq = items[-1]["seq"]
                    payload = json.dumps(items)
                    self.wfile.write(f"id: {seq}\ndata: {payload}\n\n".encode())
                else:
                    self.wfile.write(b": ping\n\n")
                self.wfile.flush()
                items = LOGS.wait(seq, 15)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass


def main():
    global PROJECT, SELF_SERVICE
    PROJECT = find_project()
    try:
        me = docker_get(f"/containers/{socket.gethostname()}/json")
        SELF_SERVICE = me["Config"]["Labels"].get("com.docker.compose.service")
    except Exception:
        SELF_SERVICE = None
    LOGS.add("observer", "observer", f"observing compose project '{PROJECT}'")
    for fn in (log_supervisor, event_watcher, status_poller):
        threading.Thread(target=fn, daemon=True).start()
    srv = http.server.ThreadingHTTPServer(("0.0.0.0", LISTEN), Handler)
    srv.daemon_threads = True
    print(f"observer listening on :{LISTEN}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
