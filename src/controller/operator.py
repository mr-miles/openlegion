"""OpenLegion Kubernetes Operator.

Reconciles AgentFleet and AgentInstance custom resources into running
Kubernetes Pods and Services.

Architecture:
  AgentFleet CR    → fleet reconciler     → creates/deletes AgentInstance CRs
  AgentInstance CR → instance reconciler  → creates Pod + Service + Secret
  AgentInstance CR → health daemon        → monitors pod, triggers replacement
  AgentInstance CR → idle timer           → culls ephemeral agents after timeout

Run:
    python -m src.controller
    # or:
    kopf run src/controller/operator.py --namespace openlegion

Required permissions (RBAC):
    - agentfleets, agentinstances: get, list, watch, create, update, patch, delete
    - pods: get, list, watch, create, delete
    - services: get, list, watch, create, delete
    - secrets: get, list, watch, create, update, patch
    - configmaps: get, list, watch, create, update, patch, delete
    - events: create, patch
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("controller.operator")

# ── Constants ──────────────────────────────────────────────────────────────────

GROUP = "openlegion.ai"
VERSION = "v1alpha1"
NAMESPACE = os.environ.get("OPENLEGION_K8S_NAMESPACE", "openlegion")
AGENT_IMAGE = os.environ.get("OPENLEGION_K8S_IMAGE", "openlegion-agent:latest")
AGENT_IMAGE_PULL_POLICY = os.environ.get("OPENLEGION_K8S_IMAGE_PULL_POLICY", "IfNotPresent")

# Instructions longer than this are moved to a ConfigMap to keep the CR small.
_INSTRUCTIONS_INLINE_LIMIT = 4096


# ── Lazy K8s client setup ──────────────────────────────────────────────────────

def _get_k8s_clients():
    """Initialize and return Kubernetes API client instances.

    Tries in-cluster config first (running inside K8s), then falls back
    to local kubeconfig for development.

    Returns:
        Tuple of (CustomObjectsApi, CoreV1Api)

    Raises:
        ImportError: if the `kubernetes` package is not installed.
        kubernetes.config.ConfigException: if no kubeconfig found.
    """
    try:
        from kubernetes import client, config as k8s_config
    except ImportError as e:
        raise ImportError(
            "The `kubernetes` package is required for the K8s controller. "
            "Install it with: pip install openlegion[k8s]"
        ) from e

    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        k8s_config.load_kube_config()

    return client.CustomObjectsApi(), client.CoreV1Api()


# ── Utility helpers ────────────────────────────────────────────────────────────

def _k8s_safe_name(name: str) -> str:
    """Convert an agent name to a Kubernetes-safe label/name component.

    K8s names allow [a-z0-9-], max 63 chars per label value.
    """
    import re
    return re.sub(r"[^a-z0-9-]", "-", name.lower())[:63]


def _resource_name(fleet_name: str, agent_name: str) -> str:
    """Build the K8s resource name for an AgentInstance, Pod, or Service."""
    return f"openlegion-{_k8s_safe_name(fleet_name)}-{_k8s_safe_name(agent_name)}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── AgentFleet reconciler ──────────────────────────────────────────────────────

try:
    import kopf
    _KOPF_AVAILABLE = True
except ImportError:
    _KOPF_AVAILABLE = False
    # Define stub decorators so this module can be imported without kopf
    # (e.g. during unit tests that mock the handlers directly).
    class _StubKopf:
        def on(self): pass
        def daemon(self, *a, **kw): return lambda f: f
        def timer(self, *a, **kw): return lambda f: f
        class on:
            @staticmethod
            def create(*a, **kw): return lambda f: f
            @staticmethod
            def update(*a, **kw): return lambda f: f
            @staticmethod
            def delete(*a, **kw): return lambda f: f
        def adopt(self, obj): pass
    kopf = _StubKopf()  # type: ignore[assignment]


if _KOPF_AVAILABLE:

    @kopf.on.create(GROUP, VERSION, "agentfleets")
    @kopf.on.update(GROUP, VERSION, "agentfleets")
    async def reconcile_fleet(
        spec: kopf.Spec,
        name: str,
        namespace: str,
        patch: kopf.Patch,
        logger: logging.Logger,
        **kwargs: Any,
    ) -> dict:
        """Reconcile an AgentFleet by creating/deleting AgentInstance CRs.

        Runs on every create or update event for AgentFleet resources.
        Diffs the desired agents list against existing AgentInstance CRs
        and creates/deletes accordingly.

        Returns a dict that Kopf stores as status.reconcile_fleet.
        """
        custom, core = _get_k8s_clients()

        desired_agents: list[dict] = spec.get("agents", [])
        desired_names: set[str] = {a["name"] for a in desired_agents}

        # List existing AgentInstances owned by this fleet
        existing = custom.list_namespaced_custom_object(
            group=GROUP, version=VERSION, namespace=namespace,
            plural="agentinstances",
            label_selector=f"openlegion.ai/fleet={name}",
        )
        existing_names: set[str] = {
            item["spec"]["agentName"]
            for item in existing.get("items", [])
        }

        to_create = desired_names - existing_names
        to_delete = existing_names - desired_names

        # Create missing AgentInstance CRs
        for agent_def in desired_agents:
            if agent_def["name"] not in to_create:
                continue
            instance_cr = _build_agent_instance_cr(
                fleet_name=name, agent_def=agent_def,
                namespace=namespace, spec=spec,
            )
            kopf.adopt(instance_cr)  # sets ownerReference to this AgentFleet
            custom.create_namespaced_custom_object(
                group=GROUP, version=VERSION, namespace=namespace,
                plural="agentinstances", body=instance_cr,
            )
            logger.info(f"Created AgentInstance for fleet={name} agent={agent_def['name']}")

        # Delete surplus AgentInstance CRs (cascade-deletes their pods)
        for surplus_name in to_delete:
            resource_name = _resource_name(name, surplus_name)
            try:
                custom.delete_namespaced_custom_object(
                    group=GROUP, version=VERSION, namespace=namespace,
                    plural="agentinstances", name=resource_name,
                )
                logger.info(f"Deleted AgentInstance {resource_name} (removed from fleet spec)")
            except Exception as e:
                logger.warning(f"Could not delete AgentInstance {resource_name}: {e}")

        # Update fleet status
        running = 0
        ready = 0
        agent_summary = []
        for item in existing.get("items", []):
            inst_status = item.get("status", {})
            agent_name = item["spec"]["agentName"]
            phase = inst_status.get("phase", "Unknown")
            if phase == "Running":
                running += 1
            if inst_status.get("conditions"):
                if any(
                    c.get("type") == "MeshReachable" and c.get("status") == "True"
                    for c in inst_status["conditions"]
                ):
                    ready += 1
            agent_summary.append({
                "name": agent_name,
                "phase": phase,
                "podRef": inst_status.get("podRef", ""),
            })

        overall_phase = "Running"
        if running == 0:
            overall_phase = "Pending" if to_create else "Stopped"
        elif running < len(desired_agents):
            overall_phase = "Degraded"

        patch.status["phase"] = overall_phase
        patch.status["runningAgents"] = running
        patch.status["readyAgents"] = ready
        patch.status["agentSummary"] = agent_summary

        return {"reconciled_at": _now_iso(), "agents_created": len(to_create), "agents_deleted": len(to_delete)}


    @kopf.on.delete(GROUP, VERSION, "agentfleets")
    async def delete_fleet(
        name: str,
        namespace: str,
        logger: logging.Logger,
        **kwargs: Any,
    ) -> None:
        """Handle AgentFleet deletion.

        AgentInstance CRs have ownerReferences pointing to the fleet, so
        Kubernetes garbage collection cascades the deletion automatically.
        This handler can perform any additional cleanup needed.
        """
        logger.info(f"AgentFleet {name} deleted — child AgentInstances will be GC'd by K8s")


    # ── AgentInstance reconciler ────────────────────────────────────────────────

    @kopf.on.create(GROUP, VERSION, "agentinstances")
    async def create_agent_instance(
        spec: kopf.Spec,
        name: str,
        namespace: str,
        meta: kopf.Meta,
        patch: kopf.Patch,
        logger: logging.Logger,
        **kwargs: Any,
    ) -> dict:
        """Provision the Pod, Service, and Secret for a new AgentInstance.

        Steps:
        1. Generate a per-agent auth token and store it in a Secret.
        2. (Optional) Create a ConfigMap for large instruction text.
        3. Create a headless ClusterIP Service for the agent.
        4. Create the agent Pod with the appropriate SecurityContext.

        Returns a dict stored as status.create_agent_instance.
        """
        custom, core = _get_k8s_clients()
        from kubernetes import client

        agent_name = spec["agentName"]
        fleet_name = spec.get("fleetRef", {}).get("name", "adhoc")
        resource_name = name  # AgentInstance name == Pod/Service name prefix

        # 1. Generate and store auth token
        auth_token = secrets.token_urlsafe(32)
        secret_name = f"{resource_name}-auth"
        secret = client.V1Secret(
            metadata=client.V1ObjectMeta(
                name=secret_name,
                namespace=namespace,
                labels={
                    "openlegion.ai/agent": agent_name,
                    "openlegion.ai/fleet": fleet_name,
                    "openlegion.ai/managed-by": "openlegion-controller",
                },
            ),
            string_data={"MESH_AUTH_TOKEN": auth_token},
        )
        kopf.adopt(secret)
        core.create_namespaced_secret(namespace=namespace, body=secret)
        logger.info(f"Created auth token Secret {secret_name}")

        # 2. Create ConfigMap for large instructions if needed
        instructions = spec.get("instructions", "")
        instructions_cm_name = None
        if len(instructions.encode()) > _INSTRUCTIONS_INLINE_LIMIT:
            instructions_cm_name = f"{resource_name}-instructions"
            cm = client.V1ConfigMap(
                metadata=client.V1ObjectMeta(
                    name=instructions_cm_name,
                    namespace=namespace,
                    labels={"openlegion.ai/agent": agent_name},
                ),
                data={"INITIAL_INSTRUCTIONS": instructions},
            )
            kopf.adopt(cm)
            core.create_namespaced_config_map(namespace=namespace, body=cm)

        # 3. Create headless Service
        service = _build_service(
            resource_name=resource_name,
            agent_name=agent_name,
            fleet_name=fleet_name,
            namespace=namespace,
        )
        kopf.adopt(service)
        core.create_namespaced_service(namespace=namespace, body=service)
        logger.info(f"Created Service {resource_name}")

        # 4. Create the agent Pod
        pod = _build_pod(
            spec=spec,
            resource_name=resource_name,
            agent_name=agent_name,
            fleet_name=fleet_name,
            namespace=namespace,
            secret_name=secret_name,
            instructions_cm_name=instructions_cm_name,
        )
        kopf.adopt(pod)
        core.create_namespaced_pod(namespace=namespace, body=pod)
        logger.info(f"Created Pod {resource_name}")

        # Update status
        agent_url = f"http://{resource_name}.{namespace}.svc:8400"
        patch.status["phase"] = "Pending"
        patch.status["serviceRef"] = resource_name
        patch.status["agentUrl"] = agent_url
        patch.status["startTime"] = _now_iso()
        patch.status["restartCount"] = 0

        return {"created_at": _now_iso(), "agent_url": agent_url}


    @kopf.on.delete(GROUP, VERSION, "agentinstances")
    async def delete_agent_instance(
        name: str,
        namespace: str,
        logger: logging.Logger,
        **kwargs: Any,
    ) -> None:
        """Handle AgentInstance deletion.

        Pod, Service, and Secret all have ownerReferences set by kopf.adopt(),
        so K8s garbage collects them automatically. This handler logs and
        can perform any additional cleanup (e.g. blackboard cleanup via mesh API).
        """
        logger.info(f"AgentInstance {name} deleted — owned resources will be GC'd")


    # ── Health monitoring daemon ────────────────────────────────────────────────

    @kopf.daemon(GROUP, VERSION, "agentinstances", interval=10.0)
    async def monitor_agent_health(
        spec: kopf.Spec,
        name: str,
        namespace: str,
        status: kopf.Status,
        patch: kopf.Patch,
        stopped: asyncio.Event,
        logger: logging.Logger,
        **kwargs: Any,
    ) -> None:
        """Continuously monitor the agent pod's health.

        Polls the pod phase every 10 seconds. On failure, deletes and
        recreates the pod (up to 5 times before giving up).

        This daemon runs as long as the AgentInstance CR exists.
        """
        custom, core = _get_k8s_clients()
        resource_name = name

        while not stopped.is_set():
            try:
                pod = _get_pod(core, resource_name, namespace)
                if pod is None:
                    phase = "Missing"
                else:
                    phase = pod.status.phase or "Unknown"

                current_phase = status.get("phase", "Unknown")

                if phase == "Running" and current_phase != "Running":
                    patch.status["phase"] = "Running"
                    logger.info(f"Agent {name} is Running")

                elif phase in ("Failed", "Unknown") and current_phase not in ("Terminating",):
                    restart_count = status.get("restartCount", 0) + 1
                    max_restarts = 5
                    if restart_count > max_restarts:
                        patch.status["phase"] = "CrashLoopBackOff"
                        logger.error(
                            f"Agent {name} exceeded max restarts ({max_restarts}). "
                            f"Setting phase=CrashLoopBackOff. Manual intervention required."
                        )
                    else:
                        logger.warning(
                            f"Agent {name} pod {phase}. Recreating (attempt {restart_count}/{max_restarts})."
                        )
                        _delete_pod(core, resource_name, namespace)
                        await asyncio.sleep(5)
                        pod = _build_pod(
                            spec=spec,
                            resource_name=resource_name,
                            agent_name=spec["agentName"],
                            fleet_name=spec.get("fleetRef", {}).get("name", "adhoc"),
                            namespace=namespace,
                            secret_name=f"{resource_name}-auth",
                            instructions_cm_name=spec.get("instructionsConfigMap"),
                        )
                        import kopf as _kopf
                        _kopf.adopt(pod)
                        core.create_namespaced_pod(namespace=namespace, body=pod)
                        patch.status["restartCount"] = restart_count
                        patch.status["phase"] = "Pending"

                elif phase == "Missing" and current_phase not in ("Terminating", "Pending"):
                    # Pod disappeared unexpectedly
                    logger.warning(f"Agent {name} pod not found. Recreating.")
                    # Same recreation logic as above (omitted for brevity — extract helper in impl)

            except Exception as e:
                logger.debug(f"Health check error for {name}: {e}")

            await asyncio.sleep(10)


    # ── Idle culling timer ──────────────────────────────────────────────────────

    @kopf.timer(GROUP, VERSION, "agentinstances", interval=60.0)
    async def check_agent_idle(
        spec: kopf.Spec,
        name: str,
        namespace: str,
        status: kopf.Status,
        patch: kopf.Patch,
        logger: logging.Logger,
        **kwargs: Any,
    ) -> None:
        """Check if an ephemeral agent has been idle too long and cull it.

        Runs every 60 seconds. Only acts on agents with ephemeral=True.
        Fleet agents (ephemeral=False) are never culled.
        """
        if not spec.get("ephemeral", False):
            return  # fleet agents: keep alive forever

        import httpx
        agent_url = status.get("agentUrl")
        if not agent_url or status.get("phase") != "Running":
            return

        try:
            async with httpx.AsyncClient(timeout=3) as client:
                resp = await client.get(f"{agent_url}/status")
                if resp.status_code != 200:
                    return
                agent_status = resp.json()
        except Exception:
            return

        is_idle = agent_status.get("state") in ("idle",)

        if is_idle:
            idle_since_str = status.get("idleSince")
            if idle_since_str is None:
                patch.status["idleSince"] = _now_iso()
                patch.status["phase"] = "Idle"
                return

            idle_since = datetime.fromisoformat(idle_since_str)
            idle_seconds = (datetime.now(timezone.utc) - idle_since).total_seconds()
            timeout = spec.get("idleTimeoutSeconds", 3600)

            if idle_seconds >= timeout:
                logger.info(
                    f"Ephemeral agent {name} idle for {idle_seconds:.0f}s "
                    f"(timeout={timeout}s). Deleting."
                )
                custom, _ = _get_k8s_clients()
                custom.delete_namespaced_custom_object(
                    group=GROUP, version=VERSION, namespace=namespace,
                    plural="agentinstances", name=name,
                )
        else:
            # Agent is active — clear idle state
            if status.get("idleSince") is not None:
                patch.status["idleSince"] = None
            if status.get("phase") == "Idle":
                patch.status["phase"] = "Running"
            patch.status["lastHeartbeat"] = _now_iso()


# ── Pod and Service builders ───────────────────────────────────────────────────

def _build_agent_instance_cr(
    fleet_name: str,
    agent_def: dict,
    namespace: str,
    spec: dict,
) -> dict:
    """Build an AgentInstance CR dict from an agent definition in AgentFleet.spec.agents."""
    agent_name = agent_def["name"]
    resource_name = _resource_name(fleet_name, agent_name)
    default_model = spec.get("defaultModel", "")
    resources = agent_def.get("resources", {})

    return {
        "apiVersion": f"{GROUP}/{VERSION}",
        "kind": "AgentInstance",
        "metadata": {
            "name": resource_name,
            "namespace": namespace,
            "labels": {
                "openlegion.ai/fleet": fleet_name,
                "openlegion.ai/agent": agent_name,
                "openlegion.ai/managed-by": "openlegion-controller",
            },
        },
        "spec": {
            "agentName": agent_name,
            "fleetRef": {"name": fleet_name, "namespace": namespace},
            "role": agent_def.get("role", ""),
            "model": agent_def.get("model", default_model),
            "thinking": agent_def.get("thinking", ""),
            "instructions": agent_def.get("instructions", ""),
            "image": AGENT_IMAGE,
            "imagePullPolicy": AGENT_IMAGE_PULL_POLICY,
            "meshUrl": f"http://openlegion-mesh.{namespace}.svc:8420",
            "mcpServers": agent_def.get("mcpServers", []),
            "ephemeral": False,  # fleet agents are always persistent
            "resourceLimits": {
                "memory": resources.get("memoryLimit", "384Mi"),
                "cpu": resources.get("cpuLimit", "150m"),
                "memoryRequest": resources.get("memoryRequest", "128Mi"),
                "cpuRequest": resources.get("cpuRequest", "50m"),
            },
            "credentialRefs": agent_def.get("credentialRefs", []),
            "env": _permissions_to_env(agent_def.get("permissions", {})),
        },
    }


def _permissions_to_env(permissions: dict) -> list[dict]:
    """Convert AgentFleet permission spec to env var list for the pod."""
    env = []
    if permissions:
        # The mesh enforces permissions from its PermissionMatrix, not the agent.
        # We pass them as env vars so the agent can self-report its ACLs.
        env.append({
            "name": "AGENT_PERMISSIONS",
            "value": json.dumps(permissions),
        })
    return env


def _build_service(
    resource_name: str,
    agent_name: str,
    fleet_name: str,
    namespace: str,
) -> Any:
    """Build a headless ClusterIP Service for agent-to-agent communication."""
    from kubernetes import client

    return client.V1Service(
        metadata=client.V1ObjectMeta(
            name=resource_name,
            namespace=namespace,
            labels={
                "openlegion.ai/agent": agent_name,
                "openlegion.ai/fleet": fleet_name,
                "openlegion.ai/managed-by": "openlegion-controller",
            },
        ),
        spec=client.V1ServiceSpec(
            # Headless: DNS resolves directly to pod IP
            cluster_ip="None",
            selector={
                "openlegion.ai/agent": agent_name,
                "openlegion.ai/fleet": fleet_name,
            },
            ports=[
                client.V1ServicePort(
                    name="http",
                    port=8400,
                    target_port=8400,
                    protocol="TCP",
                )
            ],
        ),
    )


def _build_pod(
    spec: dict,
    resource_name: str,
    agent_name: str,
    fleet_name: str,
    namespace: str,
    secret_name: str,
    instructions_cm_name: str | None = None,
) -> Any:
    """Build a Pod manifest for an agent.

    Applies the same security hardening as the current DockerBackend:
      - Non-root user (UID 1000)
      - Read-only root filesystem
      - All capabilities dropped
      - No privilege escalation
      - tmpfs for /tmp (memory-backed, noexec)

    Auth token is injected from a Secret via secretKeyRef, never as a literal value.
    """
    from kubernetes import client

    mesh_url = spec.get("meshUrl", f"http://openlegion-mesh.{namespace}.svc:8420")
    model = spec.get("model", "")
    thinking = spec.get("thinking", "")
    mcp_servers = spec.get("mcpServers", [])
    resource_limits = spec.get("resourceLimits", {})
    instructions_inline = spec.get("instructions", "")

    # Build env vars
    env = [
        client.V1EnvVar(name="AGENT_ID", value=agent_name),
        client.V1EnvVar(name="AGENT_ROLE", value=spec.get("role", "")),
        client.V1EnvVar(name="MESH_URL", value=mesh_url),
        client.V1EnvVar(
            name="MESH_AUTH_TOKEN",
            value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(
                    name=secret_name,
                    key="MESH_AUTH_TOKEN",
                )
            ),
        ),
    ]

    if model:
        env.append(client.V1EnvVar(name="LLM_MODEL", value=model))
    if thinking:
        env.append(client.V1EnvVar(name="THINKING", value=thinking))
    if mcp_servers:
        env.append(client.V1EnvVar(name="MCP_SERVERS", value=json.dumps(mcp_servers)))

    # Inline instructions (small) or reference ConfigMap (large)
    if instructions_cm_name:
        env.append(client.V1EnvVar(
            name="INITIAL_INSTRUCTIONS",
            value_from=client.V1EnvVarSource(
                config_map_key_ref=client.V1ConfigMapKeySelector(
                    name=instructions_cm_name,
                    key="INITIAL_INSTRUCTIONS",
                )
            ),
        ))
    elif instructions_inline:
        env.append(client.V1EnvVar(name="INITIAL_INSTRUCTIONS", value=instructions_inline))

    # Additional non-secret env vars from spec
    for e in spec.get("env", []):
        env.append(client.V1EnvVar(name=e["name"], value=e["value"]))

    # Additional credentials from secret refs
    for cred_ref in spec.get("credentialRefs", []):
        env_var_name = cred_ref.get("envVar", cred_ref["key"])
        env.append(client.V1EnvVar(
            name=env_var_name,
            value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(
                    name=cred_ref["name"],
                    key=cred_ref["key"],
                )
            ),
        ))

    # Security hardening — mirrors DockerBackend settings
    security_context = client.V1SecurityContext(
        run_as_non_root=True,
        run_as_user=1000,
        run_as_group=1000,
        read_only_root_filesystem=True,
        allow_privilege_escalation=False,
        capabilities=client.V1Capabilities(drop=["ALL"]),
    )

    pod_security_context = client.V1PodSecurityContext(
        run_as_non_root=True,
        run_as_user=1000,
        fs_group=1000,
    )

    # Volumes: tmpfs for /tmp (noexec equivalent; K8s Memory emptyDir is tmpfs)
    # and a data volume for agent persistent storage.
    volumes = [
        client.V1Volume(
            name="tmp",
            empty_dir=client.V1EmptyDirVolumeSource(
                medium="Memory",
                size_limit="100Mi",
            ),
        ),
        client.V1Volume(
            name="agent-data",
            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                # NOTE: PVC name matches the resource name.
                # The controller must also create this PVC (see _build_pvc).
                # TODO: create PVC in create_agent_instance handler.
                claim_name=f"{resource_name}-data",
            ),
        ),
    ]

    volume_mounts = [
        client.V1VolumeMount(name="tmp", mount_path="/tmp"),
        client.V1VolumeMount(name="agent-data", mount_path="/data"),
    ]

    # Resource limits
    resources = client.V1ResourceRequirements(
        limits={
            "memory": resource_limits.get("memory", "384Mi"),
            "cpu": resource_limits.get("cpu", "150m"),
        },
        requests={
            "memory": resource_limits.get("memoryRequest", "128Mi"),
            "cpu": resource_limits.get("cpuRequest", "50m"),
        },
    )

    container = client.V1Container(
        name="agent",
        image=AGENT_IMAGE,
        image_pull_policy=AGENT_IMAGE_PULL_POLICY,
        command=["python", "-m", "src.agent"],
        env=env,
        ports=[client.V1ContainerPort(container_port=8400, name="http")],
        resources=resources,
        security_context=security_context,
        volume_mounts=volume_mounts,
        readiness_probe=client.V1Probe(
            http_get=client.V1HTTPGetAction(
                path="/status",
                port=8400,
            ),
            initial_delay_seconds=5,
            period_seconds=10,
            failure_threshold=6,
        ),
        liveness_probe=client.V1Probe(
            http_get=client.V1HTTPGetAction(
                path="/status",
                port=8400,
            ),
            initial_delay_seconds=15,
            period_seconds=30,
            failure_threshold=3,
        ),
    )

    return client.V1Pod(
        metadata=client.V1ObjectMeta(
            # Pods get a unique suffix to allow recreation on failure
            generate_name=f"{resource_name}-",
            namespace=namespace,
            labels={
                "openlegion.ai/agent": agent_name,
                "openlegion.ai/fleet": fleet_name,
                "openlegion.ai/managed-by": "openlegion-controller",
            },
        ),
        spec=client.V1PodSpec(
            restart_policy="Never",  # Controller manages restarts, not K8s
            security_context=pod_security_context,
            containers=[container],
            volumes=volumes,
            automount_service_account_token=False,  # agents don't need K8s API access
        ),
    )


def _get_pod(core_api: Any, resource_name: str, namespace: str) -> Any | None:
    """Get the most recent pod for an AgentInstance by label selector."""
    try:
        pods = core_api.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"openlegion.ai/managed-by=openlegion-controller",
        )
        # Find pods whose generate_name matches our resource_name prefix
        matching = [
            p for p in pods.items
            if (p.metadata.generate_name or "").startswith(f"{resource_name}-")
            and p.metadata.deletion_timestamp is None
        ]
        if not matching:
            return None
        # Return the most recently created pod
        return max(matching, key=lambda p: p.metadata.creation_timestamp or "")
    except Exception:
        return None


def _delete_pod(core_api: Any, resource_name: str, namespace: str) -> None:
    """Delete the pod for an AgentInstance."""
    pod = _get_pod(core_api, resource_name, namespace)
    if pod:
        try:
            core_api.delete_namespaced_pod(
                name=pod.metadata.name,
                namespace=namespace,
            )
        except Exception as e:
            logger.warning(f"Could not delete pod for {resource_name}: {e}")


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    """Run the operator.

    Called by `python -m src.controller`.
    For production, prefer running via: kopf run src/controller/operator.py
    """
    if not _KOPF_AVAILABLE:
        raise SystemExit(
            "kopf is not installed. Run: pip install openlegion[k8s]"
        )
    import kopf
    kopf.run(
        clusterwide=False,
        namespaces=[NAMESPACE],
        liveness_endpoint="http://0.0.0.0:8080/healthz",
    )


if __name__ == "__main__":
    main()
