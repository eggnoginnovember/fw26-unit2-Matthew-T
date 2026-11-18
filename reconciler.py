# Put your code here if python is chosen for this project.

#sure
#!/usr/bin/env python3


import queue
import threading

from kubernetes import client, config, watch    
from kubernetes.client.rest import ApiException 
from rich.console import Console                #included for logging purposes

GROUP = "cosc3657.tu.ca"
PLURAL = "resilientdeployments"
KIND = "ResilientDeployment"
NAMESPACE = "default"
LABEL_KEY = "app.kubernetes.io/managed-by"   # label value = custom resource name
DEFAULT_IMAGE = "nginx:alpine"               # used if the CR has no spec.image
RESYNC_SECONDS = 10                          # safety-net full re-check interval
WATCH_TIMEOUT_SECONDS = 60                   # server-side watch timeout; we reconnect

console = Console()


def log(msg, style=None):
    # markup=False so "[RECONCILE LOOP]" prints literally instead of being parsed
    console.print(msg, style=style, markup=False, highlight=False)


class Reconciler:
    def __init__(self):
        config.load_kube_config()
        self.core = client.CoreV1Api()
        self.custom = client.CustomObjectsApi()
        self.version = self._discover_version()

        self._queue = queue.Queue()
        self._pending = set()        # names already queued (collapses event bursts)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._last_state = {}        # name -> (desired, observed) last converged state

    # ------------------------------------------------------------------ setup
    def _discover_version(self):
        """Ask the API server which version the CRD serves instead of hardcoding it."""
        ext = client.ApiextensionsV1Api()
        crd = ext.read_custom_resource_definition(f"{PLURAL}.{GROUP}")
        for v in crd.spec.versions:
            if v.storage:
                return v.name
        return crd.spec.versions[0].name

    # ------------------------------------------------------------- work queue
    def enqueue(self, name):
        with self._lock:
            if name in self._pending:
                return
            self._pending.add(name)
        self._queue.put(name)

    # --------------------------------------------------------------- watchers
    def _watch_forever(self, label, fn, args, kwargs, extract_name):
        """Stream events from the API server; reconnect on timeout or error."""
        while not self._stop.is_set():
            try:
                w = watch.Watch()
                for event in w.stream(
                    fn, *args, timeout_seconds=WATCH_TIMEOUT_SECONDS, **kwargs
                ):
                    name = extract_name(event["object"])
                    if name:
                        self.enqueue(name)
            except Exception as exc:  # network blip, expired resourceVersion, etc.
                log(f"[watch:{label}] stream interrupted ({exc!r}); reconnecting...", "yellow")
                self._stop.wait(2)

    def _resync_forever(self):
        """Periodic full list: covers startup and any missed watch events."""
        while not self._stop.is_set():
            try:
                crs = self.custom.list_namespaced_custom_object(
                    GROUP, self.version, NAMESPACE, PLURAL
                )
                for item in crs.get("items", []):
                    self.enqueue(item["metadata"]["name"])
            except Exception as exc:
                log(f"[resync] failed ({exc!r})", "yellow")
            self._stop.wait(RESYNC_SECONDS)

    # -------------------------------------------------------------- pod logic
    def _list_pods(self, name):
        return self.core.list_namespaced_pod(
            NAMESPACE, label_selector=f"{LABEL_KEY}={name}"
        ).items

    @staticmethod
    def _is_active(pod):
        """Observed state: alive = not terminating AND phase Pending or Running."""
        return pod.metadata.deletion_timestamp is None and pod.status.phase in (
            "Pending",
            "Running",
        )

    def _build_pod(self, cr):
        name = cr["metadata"]["name"]
        image = cr.get("spec", {}).get("image", DEFAULT_IMAGE)
        # ownerReference => Kubernetes garbage-collects the pods if the CR is deleted
        owner = client.V1OwnerReference(
            api_version=f"{GROUP}/{self.version}",
            kind=KIND,
            name=name,
            uid=cr["metadata"]["uid"],
            controller=True,
        )
        return client.V1Pod(
            metadata=client.V1ObjectMeta(
                generate_name=f"{name}-",          # API server appends a unique suffix
                labels={LABEL_KEY: name},
                owner_references=[owner],
            ),
            spec=client.V1PodSpec(
                containers=[client.V1Container(name="app", image=image)]
            ),
        )

    def _delete_pod(self, pod):
        try:
            self.core.delete_namespaced_pod(pod.metadata.name, NAMESPACE)
        except ApiException as exc:
            if exc.status != 404:      # already gone is fine (idempotent)
                raise

    # -------------------------------------------------------------- reconcile
    def reconcile(self, name):
        # 1. DESIRED state: always read live from the API server
        try:
            cr = self.custom.get_namespaced_custom_object(
                GROUP, self.version, NAMESPACE, PLURAL, name
            )
        except ApiException as exc:
            if exc.status == 404:      # CR deleted; owned pods are garbage-collected
                self._last_state.pop(name, None)
                return
            raise

        desired = max(0, int(cr.get("spec", {}).get("replicas", 1)))

        # 2. OBSERVED state: query the Pod API
        pods = self._list_pods(name)
        active = [p for p in pods if self._is_active(p)]
        observed = len(active)

        # 3. DRIFT
        drift = desired - observed

        # Only log when something actually needs doing or the state changed;
        # this keeps the console output identical to the README's examples.
        changed = self._last_state.get(name) != (desired, observed)

        if drift != 0 or changed:
            log(
                f"[RECONCILE LOOP] Resource: {name} | Desired: {desired} "
                f"| Observed: {observed} | Drift: {drift}"
            )

        # 4. RECONCILE
        if drift > 0:
            log(f"Scale-Up Event: Provisioning {drift} pod(s) to resolve drift...", "green")
            for _ in range(drift):
                self.core.create_namespaced_pod(NAMESPACE, self._build_pod(cr))

        elif drift < 0:
            excess = -drift
            log(f"Scale-Down Event: Pruning {excess} excess pod(s) to resolve drift...", "red")
            # Remove Pending pods first, then the newest Running ones
            victims = sorted(
                active,
                key=lambda p: (
                    p.status.phase == "Running",
                    -p.metadata.creation_timestamp.timestamp(),
                ),
            )[:excess]
            for pod in victims:
                self._delete_pod(pod)

        if drift != 0 or changed:
            log(
                f"EQUILIBRIUM REACHED: System state matches target spec for '{name}'.",
                "bold green",
            )

        # Record the converged state we just drove the system to. The pod events
        # our own create/delete calls trigger will then see (desired, desired),
        # match this, and stay silent (idempotent no-op).
        self._last_state[name] = (desired, desired)

        # Housekeeping: clear out finished/crashed pods that no longer count
        for pod in pods:
            if pod.metadata.deletion_timestamp is None and pod.status.phase in (
                "Failed",
                "Succeeded",
            ):
                self._delete_pod(pod)

    # -------------------------------------------------------------------- run
    def run(self):
        threads = [
            threading.Thread(
                target=self._watch_forever,
                args=(
                    "custom-resources",
                    self.custom.list_namespaced_custom_object,
                    (GROUP, self.version, NAMESPACE, PLURAL),
                    {},
                    lambda obj: obj["metadata"]["name"],
                ),
                daemon=True,
            ),
            threading.Thread(
                target=self._watch_forever,
                args=(
                    "pods",
                    self.core.list_namespaced_pod,
                    (NAMESPACE,),
                    {"label_selector": LABEL_KEY},
                    lambda pod: (pod.metadata.labels or {}).get(LABEL_KEY),
                ),
                daemon=True,
            ),
            threading.Thread(target=self._resync_forever, daemon=True),
        ]
        for t in threads:
            t.start()

        try:
            while True:
                try:
                    name = self._queue.get(timeout=1)   # timeout keeps Ctrl+C responsive
                except queue.Empty:
                    continue
                with self._lock:
                    self._pending.discard(name)
                try:
                    self.reconcile(name)
                except Exception as exc:
                    log(
                        f"[reconcile:{name}] error: {exc!r}; will retry on next event/resync",
                        "red",
                    )
        except KeyboardInterrupt:
            log("\nShutting down reconciler.")
            self._stop.set()


def main():
    log("=== COSC 3657 Custom Kubernetes Reconciler Started ===", "bold cyan")
    log("")
    try:
        reconciler = Reconciler()
    except ApiException as exc:
        if exc.status == 404:
            log(
                f"CRD '{PLURAL}.{GROUP}' not found. Run: kubectl apply -f crd_definition.yaml",
                "red",
            )
            return 1
        raise
    reconciler.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
