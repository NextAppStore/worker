"""Network values survive the worker's tool and message boundaries."""

import json
import shutil
from unittest.mock import MagicMock

import pytest

from app import job_queue
from app.services.opentofu_executor import OpenTofuExecutor
from app.services.packer_executor import PackerExecutor
from app.tasks import Failure, encode_packer_vars, encode_terraform_vars
from app.utils.crypto import decrypt, encrypt


def _through_job_payload(values):
    """What the backend seals on the task row and the worker opens (JSON + Fernet)."""
    return json.loads(decrypt(encrypt(json.dumps(values))))


@pytest.fixture(
    params=[
        {"ip": "192.0.2.10", "cidr": "192.0.2.0/24", "url": "http://192.0.2.10:8080"},
        {"ip": "2001:db8::10", "cidr": "2001:db8::/64", "url": "http://[2001:db8::10]:8080"},
    ]
)
def network_values(request):
    return {**request.param, "ips": ["192.0.2.10", "2001:db8::10"]}


def test_task_variables_reach_tool_arguments(mocker, tmp_path, network_values):
    """Capture actual executor argv after the job payload round-trip and task encoding."""
    received = _through_job_payload(network_values)
    tf = OpenTofuExecutor(str(tmp_path))
    stream = mocker.patch.object(tf, "_run_streamed", return_value=(True, "", ""))
    assert tf.plan(variables=encode_terraform_vars(received))[0]
    tf_args = stream.call_args.args[0]
    packer = PackerExecutor(str(tmp_path))
    run = mocker.patch("app.services.packer_executor.subprocess.run", return_value=MagicMock(returncode=0, stdout=""))
    assert packer.validate("template.pkr.hcl", encode_packer_vars(received))[0]
    for argv in (tf_args, run.call_args.args[0]):
        pairs = dict(arg.split("=", 1) for i, arg in enumerate(argv) if i > 0 and argv[i - 1] == "-var")
        for key in ("ip", "cidr", "url"):
            assert pairs[key] == network_values[key]
        assert json.loads(pairs["ips"]) == network_values["ips"]


def test_terraform_outputs_survive_success_and_failure_results(mocker, tmp_path, network_values):
    outputs = {"team_vms": {"value": {"Team-1": network_values}}}
    mocker.patch(
        "app.services.opentofu_executor.subprocess.run",
        return_value=MagicMock(
            returncode=0,
            stdout=json.dumps(outputs),
            stderr="",
        ),
    )
    result = OpenTofuExecutor(str(tmp_path)).output()
    # What lands in tasks.outputs on success and on failure.
    assert json.loads(job_queue.success_columns({"terraform_outputs": result})["outputs"]) == outputs
    failure = Failure("test", "dep", [], terraform_outputs=result)
    assert json.loads(job_queue.failure_columns(failure)["outputs"]) == outputs


@pytest.mark.integration
def test_real_opentofu_preserves_dual_stack_values(tmp_path):
    """Use the installed CLI with local state and no providers/cloud resources."""
    executor = OpenTofuExecutor(str(tmp_path))
    if shutil.which(executor.tofu_path) is None:
        pytest.skip("Requires the worker container's OpenTofu CLI")
    (tmp_path / "main.tf").write_text(
        """
variable "ips" { type = list(string) }
variable "url" { type = string }
variable "cidr" { type = string }
output "ips" { value = var.ips }
output "url" { value = var.url }
output "cidr" { value = var.cidr }
"""
    )
    values = {
        "ips": ["192.0.2.10", "2001:db8::10"],
        "url": "http://[2001:db8::10]:8080",
        "cidr": "2001:db8::/64",
    }
    success, stdout, stderr = executor.init()
    assert success, stdout + stderr
    success, stdout, stderr = executor.apply(variables=encode_terraform_vars(values))
    assert success, stdout + stderr
    assert {key: output["value"] for key, output in executor.output().items()} == values
