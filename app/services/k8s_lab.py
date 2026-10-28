"""Kubernetes-App student-pod management: ``labctl`` ported to Python.

Manifests, labels, annotations, security contexts, resource limits and the
rollout/port logic are ported 1:1 from the bash script
(``Kubernetes-App/packer/files/labctl``, namespace ``lab``) so the objects
this worker creates are indistinguishable from the ones the script would
have created on the VM.

The kubeconfig arrives from the backend already IPv4-patched (the worker's
DB role cannot read another task's ``tasks.outputs`` to re-derive the
IPv4/CA itself — see ``app.job_tables``/``f8a9b0c1d2e3_worker_role``), so
this module only builds an ``ApiClient`` from the string, it never touches
the ``server`` field.
"""

from __future__ import annotations

import contextlib
import os
import re
import secrets
import tempfile
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING

from kubernetes import client as k8s
from kubernetes import config as k8s_config
from kubernetes.client.exceptions import ApiException

if TYPE_CHECKING:
    from collections.abc import Iterator

LAB_NAMESPACE = "lab"
_FIRST_PORT = 30001
_LAST_PORT = 32767
_USERNAME_RE = re.compile(r"^[a-z]([a-z0-9-]{0,28}[a-z0-9])?$")

# Must match ``LAB_IMAGE`` in labctl and ``student_image`` in Packer, per
# Kubernetes-App's own invariant (all three agree on name and tag).
LAB_IMAGE = "kubernetes-app/student:1"

# Request = limit: every pod's resources are guaranteed (labctl's own
# invariant, checked at Terraform plan time against the VM flavor).
_POD_CPU = "0.5"
_POD_MEMORY = "1Gi"
_POD_DISK = "5Gi"
_POD_EPHEMERAL_LIMIT = "2Gi"

_ROLLOUT_POLL_INTERVAL_S = 3


class LabError(Exception):
    """A lab action failed; message is already user-facing (no secrets)."""


def validate_username(username: str) -> None:
    if not _USERNAME_RE.match(username):
        raise LabError(
            f"Invalid user name '{username}' (allowed: a-z, 0-9, -; starts with a letter, "
            "ends with a letter or digit; max. 30 characters)"
        )


def random_password(length: int = 16) -> str:
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    return "".join(secrets.choice(alphabet) for _ in range(length))


@contextmanager
def api_client_from_kubeconfig(kubeconfig_yaml: str) -> Iterator[k8s.ApiClient]:
    """Build an ``ApiClient`` from an (already IPv4-patched) kubeconfig string."""
    with tempfile.NamedTemporaryFile("w", suffix=".kubeconfig", delete=False) as f:
        f.write(kubeconfig_yaml)
        kubeconfig_path = f.name
    api_client: k8s.ApiClient | None = None
    try:
        configuration = k8s.Configuration()
        k8s_config.load_kube_config(config_file=kubeconfig_path, client_configuration=configuration)
        configuration.retries = 0
        api_client = k8s.ApiClient(configuration)
        yield api_client
    finally:
        if api_client is not None:
            with contextlib.suppress(Exception):
                api_client.close()
        with contextlib.suppress(OSError):
            os.unlink(kubeconfig_path)


def student_exists(apps_api: k8s.AppsV1Api, username: str) -> bool:
    try:
        apps_api.read_namespaced_deployment(f"student-{username}", LAB_NAMESPACE)
        return True
    except ApiException as e:
        if e.status == 404:
            return False
        raise


def generation_of(apps_api: k8s.AppsV1Api, username: str) -> int:
    try:
        dep = apps_api.read_namespaced_deployment(f"student-{username}", LAB_NAMESPACE)
    except ApiException as e:
        if e.status == 404:
            return 0
        raise
    raw = (dep.spec.template.metadata.annotations or {}).get("lab-generation")
    return int(raw) if raw and raw.isdigit() else 0


def used_ports(core_api: k8s.CoreV1Api) -> set[int]:
    services = core_api.list_namespaced_service(LAB_NAMESPACE, label_selector="role=student").items
    return {svc.spec.ports[0].node_port for svc in services if svc.spec.ports}


def next_free_port(core_api: k8s.CoreV1Api) -> int:
    used = used_ports(core_api)
    for port in range(_FIRST_PORT, _LAST_PORT + 1):
        if port not in used:
            return port
    raise LabError(f"No free NodePort left between {_FIRST_PORT} and {_LAST_PORT}")


# --- Manifests, ported from labctl's apply_pvc / apply_secret / apply_workload ---


def _pvc_manifest(username: str) -> k8s.V1PersistentVolumeClaim:
    return k8s.V1PersistentVolumeClaim(
        metadata=k8s.V1ObjectMeta(
            name=f"home-{username}",
            namespace=LAB_NAMESPACE,
            labels={"role": "student", "student": username},
        ),
        spec=k8s.V1PersistentVolumeClaimSpec(
            access_modes=["ReadWriteOnce"],
            storage_class_name="local-path",
            resources=k8s.V1VolumeResourceRequirements(requests={"storage": _POD_DISK}),
        ),
    )


def _secret_manifest(username: str, password: str) -> k8s.V1Secret:
    return k8s.V1Secret(
        metadata=k8s.V1ObjectMeta(
            name=f"student-{username}",
            namespace=LAB_NAMESPACE,
            labels={"role": "student", "student": username},
        ),
        string_data={"password": password},
    )


def _workload_manifests(username: str, port: int, generation: int) -> tuple[k8s.V1Deployment, k8s.V1Service]:
    labels = {"role": "student", "student": username}
    pod_spec = k8s.V1PodSpec(
        hostname=username,
        subdomain="students",
        automount_service_account_token=False,
        enable_service_links=False,
        termination_grace_period_seconds=5,
        security_context=k8s.V1PodSecurityContext(
            seccomp_profile=k8s.V1SeccompProfile(type="RuntimeDefault"),
            sysctls=[k8s.V1Sysctl(name="net.ipv4.ping_group_range", value="0 2147483647")],
        ),
        containers=[
            k8s.V1Container(
                name="shell",
                image=LAB_IMAGE,
                image_pull_policy="IfNotPresent",
                env=[k8s.V1EnvVar(name="STUDENT_USER", value=username)],
                ports=[k8s.V1ContainerPort(name="ssh", container_port=22)],
                security_context=k8s.V1SecurityContext(
                    allow_privilege_escalation=True,
                    privileged=False,
                    capabilities=k8s.V1Capabilities(drop=["NET_RAW", "MKNOD"]),
                ),
                resources=k8s.V1ResourceRequirements(
                    requests={"cpu": _POD_CPU, "memory": _POD_MEMORY},
                    limits={"cpu": _POD_CPU, "memory": _POD_MEMORY, "ephemeral-storage": _POD_EPHEMERAL_LIMIT},
                ),
                readiness_probe=k8s.V1Probe(
                    tcp_socket=k8s.V1TCPSocketAction(port="ssh"),
                    initial_delay_seconds=2,
                    period_seconds=3,
                ),
                volume_mounts=[
                    k8s.V1VolumeMount(name="home", mount_path=f"/home/{username}"),
                    k8s.V1VolumeMount(name="password", mount_path="/etc/student", read_only=True),
                ],
            )
        ],
        volumes=[
            k8s.V1Volume(
                name="home",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=f"home-{username}"),
            ),
            k8s.V1Volume(
                name="password",
                secret=k8s.V1SecretVolumeSource(
                    secret_name=f"student-{username}",
                    items=[k8s.V1KeyToPath(key="password", path="password")],
                ),
            ),
        ],
    )
    deployment = k8s.V1Deployment(
        metadata=k8s.V1ObjectMeta(name=f"student-{username}", namespace=LAB_NAMESPACE, labels=labels),
        spec=k8s.V1DeploymentSpec(
            replicas=1,
            strategy=k8s.V1DeploymentStrategy(type="Recreate"),
            selector=k8s.V1LabelSelector(match_labels=labels),
            template=k8s.V1PodTemplateSpec(
                metadata=k8s.V1ObjectMeta(labels=labels, annotations={"lab-generation": str(generation)}),
                spec=pod_spec,
            ),
        ),
    )
    service = k8s.V1Service(
        metadata=k8s.V1ObjectMeta(name=f"student-{username}", namespace=LAB_NAMESPACE, labels=labels),
        spec=k8s.V1ServiceSpec(
            type="NodePort",
            external_traffic_policy="Local",
            ip_family_policy="PreferDualStack",
            selector=labels,
            ports=[k8s.V1ServicePort(name="ssh", port=22, target_port="ssh", node_port=port)],
        ),
    )
    return deployment, service


def apply_pvc(core_api: k8s.CoreV1Api, username: str) -> None:
    _apply(lambda: core_api.create_namespaced_persistent_volume_claim(LAB_NAMESPACE, _pvc_manifest(username)))


def apply_secret(core_api: k8s.CoreV1Api, username: str, password: str) -> None:
    _apply(lambda: core_api.create_namespaced_secret(LAB_NAMESPACE, _secret_manifest(username, password)))


def apply_workload(apps_api: k8s.AppsV1Api, core_api: k8s.CoreV1Api, username: str, port: int, generation: int) -> None:
    deployment, service = _workload_manifests(username, port, generation)
    _apply(lambda: apps_api.create_namespaced_deployment(LAB_NAMESPACE, deployment))
    _apply(lambda: core_api.create_namespaced_service(LAB_NAMESPACE, service))


def _apply(create_call) -> None:
    """``kubectl apply`` semantics for a create-only object: create, or
    swallow 409 (already exists) — labctl's own ``kc apply`` is idempotent."""
    try:
        create_call()
    except ApiException as e:
        if e.status != 409:
            raise


def patch_generation(apps_api: k8s.AppsV1Api, username: str, generation: int) -> None:
    apps_api.patch_namespaced_deployment(
        f"student-{username}",
        LAB_NAMESPACE,
        {"spec": {"template": {"metadata": {"annotations": {"lab-generation": str(generation)}}}}},
    )


def scale(apps_api: k8s.AppsV1Api, username: str, replicas: int) -> None:
    apps_api.patch_namespaced_deployment_scale(f"student-{username}", LAB_NAMESPACE, {"spec": {"replicas": replicas}})


def scale_and_bump_generation(apps_api: k8s.AppsV1Api, username: str, replicas: int, generation: int) -> None:
    apps_api.patch_namespaced_deployment(
        f"student-{username}",
        LAB_NAMESPACE,
        {
            "spec": {
                "replicas": replicas,
                "template": {"metadata": {"annotations": {"lab-generation": str(generation)}}},
            }
        },
    )


def delete_pvc(core_api: k8s.CoreV1Api, username: str, *, wait: bool) -> None:
    try:
        core_api.delete_namespaced_persistent_volume_claim(f"home-{username}", LAB_NAMESPACE)
    except ApiException as e:
        if e.status != 404:
            raise
        return
    if wait:
        _wait_for_pvc_deleted(core_api, username)


def _wait_for_pvc_deleted(core_api: k8s.CoreV1Api, username: str, timeout_s: int = 60) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            core_api.read_namespaced_persistent_volume_claim(f"home-{username}", LAB_NAMESPACE)
        except ApiException as e:
            if e.status == 404:
                return
            raise
        time.sleep(1)


def delete_student_objects(apps_api: k8s.AppsV1Api, core_api: k8s.CoreV1Api, username: str) -> None:
    """Delete Deployment, Service and Secret (``labctl remove-student``'s first step)."""
    deletes = (
        lambda: apps_api.delete_namespaced_deployment(f"student-{username}", LAB_NAMESPACE),
        lambda: core_api.delete_namespaced_service(f"student-{username}", LAB_NAMESPACE),
        lambda: core_api.delete_namespaced_secret(f"student-{username}", LAB_NAMESPACE),
    )
    for delete in deletes:
        try:
            delete()
        except ApiException as e:
            if e.status != 404:
                raise


def wait_for_pod_deleted(core_api: k8s.CoreV1Api, username: str, timeout_s: int = 60) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        pods = core_api.list_namespaced_pod(LAB_NAMESPACE, label_selector=f"student={username}").items
        if not pods:
            return
        time.sleep(1)


def wait_for_rollout(apps_api: k8s.AppsV1Api, username: str, timeout_s: int = 180) -> None:
    """Mirrors ``labctl wait_ready``/``kubectl rollout status``: block until
    ``readyReplicas`` matches the desired replica count."""
    deadline = time.monotonic() + timeout_s
    name = f"student-{username}"
    while time.monotonic() < deadline:
        dep = apps_api.read_namespaced_deployment(name, LAB_NAMESPACE)
        desired = dep.spec.replicas or 0
        ready = dep.status.ready_replicas or 0
        if ready >= desired:
            return
        time.sleep(_ROLLOUT_POLL_INTERVAL_S)
    raise LabError(f"Timed out waiting for '{username}' to become ready ({timeout_s}s)")
