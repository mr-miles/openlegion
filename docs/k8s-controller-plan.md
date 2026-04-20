# OpenLegion Kubernetes Controller Migration Plan

**Status:** Design / Pre-implementation  
**Branch:** `k8s-controller`  
**Author:** Architecture review, April 2026  

---

## Executive Summary

OpenLegion's current container management is implemented in `src/host/runtime.py` as two backends — `DockerBackend` (bridge-networked containers) and `SandboxBackend` (microVM isolation). The mesh host directly calls the Docker SDK to start, stop, and monitor agent containers. This plan describes how to migrate to a Kubernetes controller pattern where:

1. The mesh host describes desired state as **Custom Resources** (CRs)
2. A dedicated **operator** (Kubernetes controller) reconciles those CRs into live Pods
3. The mesh host no longer calls Docker/subprocess at all — it manages CRs

The existing `RuntimeBackend` ABC is the ideal seam for this migration: we add a `KubernetesBackend` that implements the same interface but writes CRs instead of running containers.

---

## Section A: CRD Design

### A.1 AgentFleet CRD

`AgentFleet` is the cluster-scoped declaration of a named team of agents. It directly replaces the content of `src/templates/*.yaml` fleet template files, but expressed as a Kubernetes resource rather than a local YAML file loaded by the CLI.

```
apiVersion: openlegion.ai/v1alpha1
kind: AgentFleet
metadata:
  name: devteam
  namespace: openlegion
spec:
  defaultModel: claude-opus-4-5
  agents:
    - name: pm
      role: "Product manager — breaks down features, writes specs"
      model: claude-opus-4-5          # overrides fleet default
      instructions: |                  # multiline OK, stored as ConfigMap ref
        ...
      soul: |
        ...
      heartbeat: |
        ...
      permissions:
        blackboardRead: ["tasks/*", "status/*"]
        blackboardWrite: ["tasks/*", "status/*"]
        canPublish: ["tasks_ready"]
        canSubscribe: ["task_complete"]
      resources:
        memoryLimit: "512Mi"
        cpuLimit: "500m"
        memoryRequest: "128Mi"
        cpuRequest: "50m"
      budget:
        dailyUsd: 5.0
        monthlyUsd: 100.0
      mcpServers: []                   # list of MCP server configs
      thinking: ""                     # extended thinking mode
      credentialRefs:                  # names of K8s Secrets to mount
        - name: anthropic-api-key
          key: ANTHROPIC_API_KEY
status:
  observedGeneration: 1
  runningAgents: 3
  readyAgents: 2
  phase: Running                       # Pending | Running | Degraded | Stopped
  conditions:
    - type: Ready
      status: "True"
      lastTransitionTime: "2026-04-20T10:00:00Z"
      reason: AllAgentsReady
      message: "3/3 agents running"
  agentSummary:
    - name: pm
      phase: Running
      podRef: "openlegion/openlegion-devteam-pm-xk9q2"
    - name: engineer
      phase: Running
      podRef: "openlegion/openlegion-devteam-engineer-p7f4z"
```

**Field notes:**

- `instructions`, `soul`, `heartbeat` are stored verbatim in the CRD spec. For large values (>512 bytes), the controller creates a `ConfigMap` and mounts it into the pod, keeping the CR spec manageable.
- `credentialRefs` lists references to K8s Secrets. The controller never reads the secret values — it creates a projected volume in the pod spec. Secrets are never embedded in the CR.
- `resources` maps directly to the pod's container resource requests/limits. The defaults mirror current Docker hardening: `mem_limit="384m"`, `cpu_quota=15000` (0.15 CPU).
- `status` is written by the controller only, via the `/status` subresource. The mesh host never writes to it.

### A.2 AgentInstance CRD

`AgentInstance` represents a single running agent pod. `AgentFleet` references describe what the user wants; `AgentInstance` resources describe what the controller has actually created. The controller creates one `AgentInstance` per agent defined in the fleet.

```
apiVersion: openlegion.ai/v1alpha1
kind: AgentInstance
metadata:
  name: openlegion-devteam-pm
  namespace: openlegion
  ownerReferences:
    - apiVersion: openlegion.ai/v1alpha1
      kind: AgentFleet
      name: devteam
      uid: <fleet-uid>
      controller: true
      blockOwnerDeletion: true
  labels:
    openlegion.ai/fleet: devteam
    openlegion.ai/agent: pm
    openlegion.ai/role: product-manager
spec:
  agentName: pm
  fleetRef:
    name: devteam
    namespace: openlegion
  model: claude-opus-4-5
  role: "Product manager..."
  image: openlegion-agent:latest
  imagePullPolicy: IfNotPresent
  env:
    - name: AGENT_ID
      value: pm
    - name: AGENT_ROLE
      value: "Product manager..."
    - name: MESH_URL
      value: "http://openlegion-mesh:8420"
  resourceLimits:
    memory: "512Mi"
    cpu: "500m"
  credentialRefs:
    - name: anthropic-api-key
      key: ANTHROPIC_API_KEY
  authTokenRef:
    secretName: openlegion-agent-tokens   # controller generates per-agent tokens
    key: pm
status:
  phase: Running                   # Pending | Running | Idle | CrashLoopBackOff | Terminating
  podRef: "openlegion-devteam-pm-xk9q2"
  podIP: "10.0.1.42"
  serviceRef: "openlegion-devteam-pm"
  lastHeartbeat: "2026-04-20T10:05:00Z"
  startTime: "2026-04-20T09:58:00Z"
  restartCount: 0
  conditions:
    - type: PodReady
      status: "True"
    - type: MeshReachable
      status: "True"
```

**Field notes:**

- `ownerReferences` with `controller: true` means Kubernetes garbage-collects `AgentInstance` objects (and their child Pods and Services) automatically when the parent `AgentFleet` is deleted.
- `authTokenRef` points to a Secret that the controller creates and rotates. Each agent gets a unique `MESH_AUTH_TOKEN`; the controller writes tokens into the Secret and the pod reads them via `valueFrom.secretKeyRef`.
- `phase: Idle` is a custom extension — set by the health monitor reconciler when the pod is running but has not processed a request in `idleTimeoutSeconds` (default 3600). Idle agents can be culled by a lower-priority reconciler loop.

### A.3 CRD Validation Schema

Both CRDs use OpenAPI v3 validation in their spec. Key constraints (mirroring existing Docker runtime limits):

| Field | Constraint |
|---|---|
| `agentName` | `^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$` (mirrors `AGENT_ID_RE_PATTERN`) |
| `resources.memoryLimit` | Must be valid K8s quantity; default `384Mi` |
| `resources.cpuLimit` | Must be valid K8s quantity; default `150m` |
| `budget.dailyUsd` | `minimum: 0.0` |
| `mcpServers` | Array, each item has `name` (string) and `config` (object) |

---

## Section B: Controller Architecture

### B.1 Framework Recommendation: Kopf

**Recommended framework: Kopf** (Python Kubernetes Operator Framework by Zalando)

Justification:

1. **Language match**: OpenLegion's entire stack is Python. A Kopf operator lives in the same repo, same Docker image build, same CI pipeline, and can import `src.shared.types` for Pydantic model validation without any FFI bridge.
2. **Minimal boilerplate**: A functional Kopf operator is ~50 lines. Handler registration is decorator-based, directly analogous to FastAPI route registration that the team already uses.
3. **Feature completeness**: Kopf handles owner references, finalizers, status subresource writes, daemon handlers (long-running per-resource coroutines), timer handlers (periodic reconciliation), and diff-aware update handlers — everything this use case requires.
4. **Stable**: v1.44.5 released April 2026, 2.6k GitHub stars, Python 3.10+ compatible. Maintainer explicitly describes it as "production-ready and stable (semantic v1)." No major new features planned — it does exactly what it says.
5. **No CRD auto-registration**: Kopf does NOT apply CRDs to the cluster on its own — CRDs must be applied separately (via `kubectl apply -f k8s/crds/`). This is actually desirable: CRD schema is managed deliberately, not silently mutated by a running operator.

**Framework comparison table:**

| Framework | Language | CRD mgmt | Boilerplate | Active? | Best for |
|---|---|---|---|---|---|
| **Kopf** | Python | Manual (intentional) | Very low | Yes (v1.44.5) | Python shops, rapid iteration |
| Operator SDK (Go) | Go | Scaffolded | High | Yes | Production Go operators, full OLM lifecycle |
| Operator SDK (Helm) | Helm/YAML | Via chart | Very low | Yes | Teams with existing Helm charts |
| kubebuilder | Go | Scaffolded | High | Yes | Go, maximum control |
| KEDA | YAML/any | Not applicable | None (config only) | Yes | Scaling existing workloads from queues |
| Argo Workflows | YAML/Go | Not applicable | Medium | Yes | Complex DAG workflows, CI/CD |
| Crossplane | YAML | Via composition | Very high | Yes | Cloud infrastructure (not pods) |

**Why not the alternatives:**

- **Operator SDK (Helm)**: Would require expressing agent config as Helm values, not Python dataclasses. Loses type safety. Helm templating is a poor fit for the dynamic, runtime-generated pod specs (per-agent auth tokens, dynamic port assignments, etc.).
- **Operator SDK (Go)**: Correct choice if OpenLegion were Go. Introduces a second language in the repo, separate build pipeline, separate CI. The cognitive overhead is not justified.
- **KEDA**: Excellent for scaling a fixed pool of workers based on queue depth, but it does not manage heterogeneous pod specs (different roles, models, prompts per agent). It cannot replace custom lifecycle logic. KEDA could be layered on top as an optional scaling enhancement post-migration.
- **Argo Workflows**: Designed for DAG-structured batch jobs. OpenLegion agents are long-running services, not tasks with defined start/end. Argo's overhead (workflow controller, Argo Server, artifact repository) would be significant for what is essentially a pod lifecycle manager.
- **Crossplane**: Designed for composing cloud infrastructure (RDS, GKE clusters, etc.) via CRDs. Not appropriate for managing in-cluster pods.

### B.2 Controller Reconcile Loop

The operator runs as a single Deployment in the `openlegion` namespace. It watches two resource types:

**AgentFleet reconciler** (primary):

```
@kopf.on.create('openlegion.ai', 'v1alpha1', 'agentfleets')
@kopf.on.update('openlegion.ai', 'v1alpha1', 'agentfleets')
async def reconcile_fleet(spec, name, namespace, **kwargs):
    desired_agents = spec['agents']
    existing_instances = list_agent_instances(fleet=name, namespace=namespace)

    # Diff: create missing, delete surplus
    desired_names = {a['name'] for a in desired_agents}
    existing_names = {i.metadata.name for i in existing_instances}

    to_create = desired_names - existing_names
    to_delete = existing_names - desired_names
    to_update = desired_names & existing_names  # check spec hash

    for agent_name in to_create:
        agent_spec = get_agent_spec(desired_agents, agent_name)
        create_agent_instance(fleet_name=name, agent_spec=agent_spec, namespace=namespace)
        create_agent_service(fleet_name=name, agent_name=agent_name, namespace=namespace)

    for agent_name in to_delete:
        delete_agent_instance(fleet_name=name, agent_name=agent_name, namespace=namespace)

    for agent_name in to_update:
        if spec_changed(desired_agents, existing_instances, agent_name):
            patch_agent_instance(fleet_name=name, agent_name=agent_name, namespace=namespace)

    update_fleet_status(name, namespace)
```

**AgentInstance reconciler** (secondary):

```
@kopf.on.create('openlegion.ai', 'v1alpha1', 'agentinstances')
async def reconcile_agent_instance(spec, name, namespace, **kwargs):
    # Generate per-agent auth token, store in Secret
    ensure_auth_token_secret(agent_name=spec['agentName'], namespace=namespace)

    # Create ConfigMaps for large text fields (instructions, soul, etc.)
    ensure_agent_configmaps(spec=spec, namespace=namespace)

    # Create the Pod
    pod = build_pod_spec(spec)
    kopf.adopt(pod)  # sets ownerReference to the AgentInstance
    kubernetes_client.create_namespaced_pod(namespace=namespace, body=pod)
```

**Pod watcher daemon** (per AgentInstance):

```
@kopf.daemon('openlegion.ai', 'v1alpha1', 'agentinstances')
async def watch_pod_health(spec, name, namespace, stopped, **kwargs):
    while not stopped.is_set():
        pod = get_pod(name, namespace)
        phase = pod.status.phase if pod else 'Missing'
        patch_status(name, namespace, {'phase': phase})

        if phase == 'Failed':
            # Trigger pod replacement
            delete_pod(name, namespace)
            await asyncio.sleep(5)
            recreate_pod(spec, name, namespace)

        await asyncio.sleep(10)
```

### B.3 Health, Liveness, and Idle Culling

**Liveness**: The controller's pod watcher daemon (above) detects pod crashes and replaces them. Pod restart policy is set to `Never` so Kubernetes does not restart on its own — the controller has full ownership of pod replacement decisions (prevents thundering herd on credential errors).

**Idle culling**: The `src/host/health.py` health monitor already polls agents via HTTP `/status`. In the K8s world, this runs as a periodic Kopf timer:

```
@kopf.timer('openlegion.ai', 'v1alpha1', 'agentinstances', interval=60.0)
async def check_agent_idle(spec, name, namespace, status, **kwargs):
    agent_url = f"http://{status['serviceRef']}.{namespace}.svc:8400"
    agent_status = await poll_agent_status(agent_url)

    if agent_status.state == 'idle':
        idle_since = status.get('idleSince') or now_iso()
        idle_seconds = (now() - parse_iso(idle_since)).total_seconds()
        patch_status(name, namespace, {'idleSince': idle_since})

        idle_threshold = spec.get('idleTimeoutSeconds', 3600)
        is_ephemeral = spec.get('ephemeral', False)
        if is_ephemeral and idle_seconds > idle_threshold:
            # Delete the AgentInstance CR — cascade deletes pod + service
            delete_agent_instance(name, namespace)
    else:
        patch_status(name, namespace, {'idleSince': None, 'lastHeartbeat': now_iso()})
```

Persistent agents (non-ephemeral) are never culled. Only agents spawned via the `spawn` tool (ephemeral=True, TTL-based) are culled on idle.

**Keep-warm**: Fleet agents defined in `AgentFleet.spec.agents` always have `ephemeral: false`. The controller ensures they are always running (recreates if pod dies). This matches the current behavior where named fleet agents are started at launch and kept alive.

### B.4 Scaling

Two scaling modes:

1. **Fleet-level scaling** (manual): User updates `AgentFleet.spec.agents` to add/remove agent definitions. The controller diff reconciles this into pod create/delete operations. This is equivalent to adding agents in the current dashboard.

2. **KEDA integration** (optional, Phase 3): A `ScaledObject` can target a pool of identical worker `AgentInstance` resources. The `KedaScaler` CR watches a Redis list (task queue) and scales the number of `AgentInstance` replicas for a given role. This requires extracting the task queue concept from blackboard into a proper queue primitive — out of scope for Phase 1-2.

---

## Section C: Mesh Host Changes

### C.1 New KubernetesBackend

The cleanest migration path is adding a third `RuntimeBackend` implementation. The existing `DockerBackend` and `SandboxBackend` remain completely unchanged. The `select_backend()` factory in `runtime.py` gains a third option:

```python
# src/host/runtime.py addition

class KubernetesBackend(RuntimeBackend):
    """Runs agents as Kubernetes AgentInstance CRs.
    
    The controller (src/controller/operator.py) handles actual pod creation.
    This backend is a CR producer only — it writes AgentInstance objects
    and reads their status. No direct Docker or subprocess calls.
    """

    def __init__(
        self,
        mesh_host_port: int = 8420,
        namespace: str = "openlegion",
        project_root: str | None = None,
    ):
        super().__init__(mesh_host_port=mesh_host_port, project_root=project_root)
        from kubernetes import client, config as k8s_config
        try:
            k8s_config.load_incluster_config()  # running inside K8s
        except k8s_config.ConfigException:
            k8s_config.load_kube_config()       # local dev with kubeconfig
        self._k8s_custom = client.CustomObjectsApi()
        self._k8s_core = client.CoreV1Api()
        self._namespace = namespace
        self._group = "openlegion.ai"
        self._version = "v1alpha1"

    def start_agent(self, agent_id, role, skills_dir, system_prompt="",
                    model="", mcp_servers=None, thinking="",
                    env_overrides=None) -> str:
        """Create an AgentInstance CR. Returns the ClusterIP service URL."""
        auth_token = secrets.token_urlsafe(32)
        self.auth_tokens[agent_id] = auth_token

        instance_spec = {
            "apiVersion": f"{self._group}/{self._version}",
            "kind": "AgentInstance",
            "metadata": {
                "name": f"openlegion-adhoc-{_k8s_safe_name(agent_id)}",
                "namespace": self._namespace,
                "labels": {
                    "openlegion.ai/agent": agent_id,
                    "openlegion.ai/managed-by": "mesh-host",
                },
            },
            "spec": {
                "agentName": agent_id,
                "role": role,
                "model": model or "",
                "thinking": thinking or "",
                "mcpServers": mcp_servers or [],
                "ephemeral": True,
                "meshUrl": f"http://openlegion-mesh:{self.mesh_host_port}",
                "authToken": auth_token,  # controller stores in Secret
            }
        }
        self._k8s_custom.create_namespaced_custom_object(
            group=self._group, version=self._version,
            namespace=self._namespace,
            plural="agentinstances", body=instance_spec,
        )

        svc_name = f"openlegion-adhoc-{_k8s_safe_name(agent_id)}"
        url = f"http://{svc_name}.{self._namespace}.svc:8400"
        self.agents[agent_id] = {"url": url, "role": role}
        return url

    def stop_agent(self, agent_id, *, remove_data=False):
        """Delete the AgentInstance CR — controller cascade-deletes pod+service."""
        safe = _k8s_safe_name(agent_id)
        self._k8s_custom.delete_namespaced_custom_object(
            group=self._group, version=self._version,
            namespace=self._namespace,
            plural="agentinstances",
            name=f"openlegion-adhoc-{safe}",
        )
        self.agents.pop(agent_id, None)
        self.auth_tokens.pop(agent_id, None)

    def health_check(self, agent_id: str) -> bool:
        """Check AgentInstance status phase."""
        try:
            safe = _k8s_safe_name(agent_id)
            obj = self._k8s_custom.get_namespaced_custom_object(
                group=self._group, version=self._version,
                namespace=self._namespace,
                plural="agentinstances",
                name=f"openlegion-adhoc-{safe}",
            )
            return obj.get("status", {}).get("phase") == "Running"
        except Exception:
            return False

    # ... get_logs, wait_for_agent implemented similarly

    @staticmethod
    def backend_name() -> str:
        return "kubernetes"
```

**What is removed from the mesh host**: Nothing in Phase 1. Docker imports remain. The `select_backend()` function gains `OPENLEGION_BACKEND=kubernetes` as a new option.

**What the mesh host no longer does** (after Phase 3):
- No `docker.from_env()` calls
- No port tracking (`_next_port` / `_port_lock`)
- No network creation (`_ensure_agent_network`)
- No volume management
- No `host.docker.internal` host-entry injection
- No `platform.system()` branching for Windows paths
- No `pids_limit`, `mem_limit`, `cpu_quota` Docker-specific knobs (moved to CRD spec, enforced by K8s resource limits)

### C.2 Fleet Loading

Currently, `src/cli/config.py:_load_templates()` reads YAML files from `src/templates/` and `src/cli/config.py:_create_agent_from_template()` calls `container_manager.start_agent()` for each agent.

In the K8s model, fleet loading becomes applying a `AgentFleet` CR. The CLI gains a new path:

```python
# src/cli/config.py addition
def apply_fleet_template_to_k8s(template_name: str, namespace: str = "openlegion"):
    """Convert a fleet template YAML to an AgentFleet CR and apply it."""
    template = _load_template(template_name)
    fleet_cr = template_to_fleet_cr(template, namespace=namespace)
    backend = select_backend()  # KubernetesBackend
    backend.apply_fleet(fleet_cr)
```

### C.3 What Stays in the Mesh Host

The mesh host retains full responsibility for:
- Blackboard (SQLite), PubSub, MessageRouter — unchanged
- Credential vault — unchanged
- Permission enforcement — unchanged
- Cost tracking — unchanged
- Cron scheduler — unchanged
- Health monitor polling (now reads from AgentInstance status via K8s API instead of Docker container state)
- Dashboard and all FastAPI endpoints — unchanged
- LLM proxy — unchanged
- Browser service management — Docker for now (browser service is not multi-tenant per agent, it is shared; this can remain Docker longer)

The mesh host no longer needs:
- `docker` Python package (after Phase 3)
- Port allocation logic (K8s Services get stable DNS names, no host port assignment needed)
- Network management (K8s handles networking)

---

## Section D: Networking

### D.1 Agent-to-Mesh Communication

In Docker mode, agents reach the mesh at `http://host.docker.internal:8420`.

In Kubernetes mode, the mesh host runs as a Deployment + Service named `openlegion-mesh` in the `openlegion` namespace. Agents use:

```
MESH_URL=http://openlegion-mesh.openlegion.svc.cluster.local:8420
```

This is set as a fixed env var in every agent pod spec. No host.docker.internal quirks, no `extra_hosts` injection, no Linux vs macOS branching.

### D.2 Mesh-to-Agent Communication

Each `AgentInstance` gets a headless Kubernetes Service (not a LoadBalancer or NodePort):

```yaml
apiVersion: v1
kind: Service
metadata:
  name: openlegion-devteam-pm
  namespace: openlegion
  labels:
    openlegion.ai/agent: pm
    openlegion.ai/fleet: devteam
spec:
  selector:
    openlegion.ai/agent: pm
    openlegion.ai/fleet: devteam
  ports:
    - port: 8400
      targetPort: 8400
  clusterIP: None   # headless — direct pod DNS
```

The `MessageRouter` in `src/host/mesh.py` currently resolves agent IDs to URLs. In K8s mode, the URL is `http://openlegion-devteam-pm.openlegion.svc:8400`. The `KubernetesBackend.start_agent()` returns this URL, and `router.register_agent()` stores it. No other changes to the router are required.

**Why headless services**: Agents have single-pod instances (no replicas per agent identity). Headless services resolve directly to pod IP, avoiding an unnecessary iptables hop. Also future-safe: if an agent pod is replaced, the new pod re-registers with the same selector labels and the DNS record updates within the standard K8s TTL.

### D.3 External Access (User → Mesh)

The mesh host (port 8420) needs to be reachable from:
- CLI (`openlegion` command running on the user's machine)
- Messaging channel bots (Telegram, Discord, Slack, WhatsApp webhook)
- Dashboard browser

Options in order of preference:

1. **NodePort Service** (development): Exposes port 8420 on every cluster node. Simple, no extra components.
2. **Ingress with TLS** (production): Nginx or Traefik ingress controller routes `https://your-domain/` to the mesh service. This is the existing Caddy/SSO pattern from the provisioner, adapted to K8s Ingress.
3. **LoadBalancer Service** (cloud-managed): Cloud LB directly to the mesh service. Simplest for managed K8s (GKE, EKS, AKS).

### D.4 Browser Service

The shared browser service container (`Dockerfile.browser`) is a special case:
- It has `NET_ADMIN` capability for iptables egress filtering
- It is shared across all agents (not per-agent)
- KasmVNC requires specific port exposure and SHM configuration

The browser service stays as a Docker container or moves to a separate K8s Deployment with a custom SecurityContext granting `NET_ADMIN`. This is a separate migration item, not required for Phase 1-2.

### D.5 Service Mesh (Istio/Linkerd)

**Recommendation: skip for now.** Service meshes add mutual TLS between pods, traffic shaping, and observability. OpenLegion already implements its own auth (MESH_AUTH_TOKEN per agent, verified on every mesh request). Adding Istio would double the auth mechanism without adding meaningfully more security given the existing design. Revisit if the cluster grows to multi-tenant or if mTLS between agents becomes a requirement.

---

## Section E: Migration Path

### Phase 1 — KubernetesBackend alongside Docker (feature flag)

**Goal**: Prove the K8s integration works without breaking anything.

**Changes**:
- Add `src/controller/__init__.py` and `src/controller/operator.py` (Kopf operator)
- Add `k8s/crds/agentfleet.yaml` and `k8s/crds/agentinstance.yaml`
- Add `class KubernetesBackend(RuntimeBackend)` to `src/host/runtime.py`
- Add `OPENLEGION_BACKEND=kubernetes` env var to `select_backend()` factory
- Add `kubernetes` Python package to `pyproject.toml` optional deps (`k8s` group)
- Add `kopf` to `pyproject.toml` optional deps (`k8s` group)
- Controller deployed as a separate pod in the cluster

**Feature flag**: `OPENLEGION_BACKEND=docker` (default, existing behavior unchanged) or `OPENLEGION_BACKEND=kubernetes`.

**Testing**: Add `tests/test_k8s_backend.py` using `unittest.mock` to mock the `kubernetes.client` API calls. No live cluster required for CI.

**Deliverables**:
- CRD files installable via `kubectl apply -f k8s/crds/`
- Operator runs via `python -m src.controller` or `kopf run src/controller/operator.py`
- Manual test: start mesh with `OPENLEGION_BACKEND=kubernetes`, verify agent pod is created when a fleet is loaded

### Phase 2 — Default to Kubernetes, Docker as fallback

**Goal**: Kubernetes is the default on clusters; Docker is the fallback for local dev.

**Changes**:
- `select_backend()` auto-detects cluster environment: if running in-cluster (check `KUBERNETES_SERVICE_HOST` env var) and `OPENLEGION_BACKEND` is not explicitly `docker`, use `KubernetesBackend`
- Local dev without K8s continues to use `DockerBackend` automatically
- Fleet template YAML files dual-published: as local files (for Docker mode) and as `AgentFleet` CRs (for K8s mode)
- Dashboard gains K8s-aware status view: reads `AgentInstance` status instead of Docker container state
- `src/host/health.py` `HealthMonitor` gains a K8s branch that reads pod phase from `AgentInstance.status.phase`

**Deliverables**:
- Helm chart (or Kustomize overlay) for deploying the full stack (mesh + controller + CRDs)
- Updated `install.sh` / `install.ps1` with K8s install path
- Updated QUICKSTART.md with K8s section

### Phase 3 — Remove Docker management

**Goal**: All Docker SDK code removed from the agent lifecycle path.

**Changes**:
- Delete `class DockerBackend` from `src/host/runtime.py` (or retain as a standalone module for non-K8s users)
- Delete `class SandboxBackend` from `src/host/runtime.py`
- Remove `docker` Python dependency from `pyproject.toml` core deps (move to optional `docker` group for users who still want single-machine mode)
- Remove port allocation logic, network management, Linux/macOS/Windows branching
- Remove `containers.py` backward-compat alias (only consumed by E2E tests, which gain K8s E2E tests)
- Browser service migrated to K8s Deployment with SecurityContext
- Full Helm chart covers all components

**Note**: Do not delete `DockerBackend` until all users have migrated. Keep it behind an optional dependency group for at least one major version.

---

## Section F: Security Considerations in K8s

The current Docker hardening (`no-new-privileges`, `cap_drop: ALL`, `read_only: True`, `tmpfs: /tmp`) maps directly to Kubernetes SecurityContext:

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 1000
  runAsGroup: 1000
  readOnlyRootFilesystem: true
  allowPrivilegeEscalation: false
  capabilities:
    drop: ["ALL"]

volumes:
  - name: tmp
    emptyDir:
      medium: Memory
      sizeLimit: 100Mi

volumeMounts:
  - name: tmp
    mountPath: /tmp
```

**PID limits**: Use `PodSpec.securityContext.sysctls` or admission webhook. K8s does not expose `pids_limit` natively in the spec but it can be enforced via cgroup via the container runtime (containerd/CRI-O). For now, accept the loss of explicit PID limit; it is less critical than memory/CPU limits which are enforced natively.

**Credential injection**: Current Docker backend passes `MESH_AUTH_TOKEN` as a plain env var. In K8s, auth tokens are stored in a `Secret` named `openlegion-agent-tokens` and injected via `valueFrom.secretKeyRef`. LLM API keys from the credential vault are NOT injected into agent pods — agents never hold keys, and this invariant is preserved. The mesh service (`openlegion-mesh`) holds the credential vault.

**Network policy**: Apply a `NetworkPolicy` that allows:
- Agent pods → mesh service (port 8420)
- Mesh pod → agent pods (port 8400)
- Agent pods → internet (for web_search, HTTP tools)
- Deny: agent pod → agent pod directly (agents must route through mesh)

---

## Section G: Implementation Checklist

For the developer picking this up:

**Week 1 — CRDs and skeleton operator**
- [ ] Apply CRD YAMLs to dev cluster: `kubectl apply -f k8s/crds/`
- [ ] Implement `src/controller/operator.py` AgentFleet reconciler (create/delete instances)
- [ ] Implement AgentInstance reconciler (create pod + service)
- [ ] Test: apply a sample `AgentFleet` CR, verify pods appear

**Week 2 — KubernetesBackend in mesh host**
- [ ] Implement `KubernetesBackend` in `src/host/runtime.py`
- [ ] Wire `OPENLEGION_BACKEND=kubernetes` into `select_backend()`
- [ ] Add `kubernetes` and `kopf` to `pyproject.toml` optional deps
- [ ] Write `tests/test_k8s_backend.py` with mocked K8s client

**Week 3 — Integration and health**
- [ ] Controller daemon handler for pod health monitoring
- [ ] Idle culling timer handler
- [ ] Auth token generation in controller (Secret creation)
- [ ] ConfigMap creation for large text fields
- [ ] Update `src/host/health.py` to support K8s phase reading

**Week 4 — Network and deployment**
- [ ] Service creation per AgentInstance
- [ ] NetworkPolicy
- [ ] Helm chart skeleton (or Kustomize)
- [ ] Manual end-to-end test with full fleet

---

## Appendix: Key File Mapping

| Current (Docker) | K8s Equivalent |
|---|---|
| `src/host/runtime.py:DockerBackend` | `src/host/runtime.py:KubernetesBackend` + `src/controller/operator.py` |
| `src/host/containers.py` (alias) | No equivalent needed |
| `src/templates/*.yaml` | `AgentFleet` CRs in `k8s/fleets/*.yaml` |
| Docker bridge network `openlegion_agents` | K8s `NetworkPolicy` + ClusterIP Services |
| `openlegion_data_{agent}` Docker volume | K8s `PersistentVolumeClaim` per AgentInstance |
| `host.docker.internal` hostname | `openlegion-mesh.openlegion.svc.cluster.local` |
| `_next_port` port counter | Not needed (K8s DNS replaces host port mapping) |
| `MESH_AUTH_TOKEN` env var (plain) | K8s Secret → `valueFrom.secretKeyRef` |
| `docker.from_env()` | `kubernetes.client.CustomObjectsApi()` |

## Appendix: Environment Variables for KubernetesBackend

| Variable | Default | Description |
|---|---|---|
| `OPENLEGION_BACKEND` | `docker` | Set to `kubernetes` to use K8s controller |
| `OPENLEGION_K8S_NAMESPACE` | `openlegion` | K8s namespace for agent resources |
| `OPENLEGION_K8S_IMAGE` | `openlegion-agent:latest` | Container image for agent pods |
| `OPENLEGION_K8S_IMAGE_PULL_POLICY` | `IfNotPresent` | K8s imagePullPolicy |
| `OPENLEGION_K8S_STORAGE_CLASS` | `""` | StorageClass for agent PVCs (empty = default) |
| `KUBERNETES_SERVICE_HOST` | (set by K8s) | Auto-detected: if set, in-cluster config is used |
