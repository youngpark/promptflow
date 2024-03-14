# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------
import json
import socket
import subprocess
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from promptflow._core._errors import UnexpectedError
from promptflow._sdk._constants import FLOW_META_JSON, FLOW_META_JSON_GEN_TIMEOUT, PROMPT_FLOW_DIR_NAME
from promptflow.batch._csharp_base_executor_proxy import CSharpBaseExecutorProxy
from promptflow.storage._run_storage import AbstractRunStorage

EXECUTOR_SERVICE_DOMAIN = "http://localhost:"
EXECUTOR_SERVICE_DLL = "Promptflow.dll"


class CSharpExecutorProxy(CSharpBaseExecutorProxy):
    def __init__(
        self,
        *,
        process,
        port: str,
        working_dir: Optional[Path] = None,
        chat_output_name: Optional[str] = None,
        enable_stream_output: bool = False,
    ):
        self._process = process
        self._port = port
        self._chat_output_name = chat_output_name
        super().__init__(
            working_dir=working_dir,
            enable_stream_output=enable_stream_output,
        )

    @property
    def api_endpoint(self) -> str:
        return EXECUTOR_SERVICE_DOMAIN + self.port

    @property
    def port(self) -> str:
        return self._port

    @property
    def chat_output_name(self) -> Optional[str]:
        return self._chat_output_name

    @classmethod
    def generate_flow_metadata(
        cls,
        flow_file: Path,
        working_dir: Path,
        dump: bool = True,
        timeout: int = FLOW_META_JSON_GEN_TIMEOUT,
        load_in_subprocess: bool = True,
    ) -> Dict[str, Any]:
        # TODO: timeout & dump doesn't take effect for now
        # TODO: provide a way to skip dumping and directly read from flow.json
        cls._dump_metadata(
            flow_file=flow_file,
            working_dir=working_dir,
        )

        from promptflow import load_flow
        from promptflow._sdk.entities._eager_flow import EagerFlow

        flow = load_flow(flow_file)
        if isinstance(flow, EagerFlow):
            return json.load((working_dir / PROMPT_FLOW_DIR_NAME / FLOW_META_JSON).open())
        else:
            return {}

    @classmethod
    def _dump_metadata(cls, flow_file: Path, working_dir: Path):
        """In csharp, we need to generate metadata based on a dotnet command for now and the metadata will
        always be dumped.
        """
        command = [
            "dotnet",
            EXECUTOR_SERVICE_DLL,
            "--flow_meta",
            "--yaml_path",
            flow_file.absolute().as_posix(),
            "--assembly_folder",
            ".",
        ]
        try:
            subprocess.check_output(
                command,
                cwd=working_dir,
            )
        except subprocess.CalledProcessError as e:
            raise UnexpectedError(
                message_format="Failed to generate flow meta for csharp flow.\n"
                "Command: {command}\n"
                "Working directory: {working_directory}\n"
                "Return code: {return_code}\n"
                "Output: {output}",
                command=" ".join(command),
                working_directory=working_dir.as_posix(),
                return_code=e.returncode,
                output=e.output,
            )

    @classmethod
    def get_outputs_definition(cls, flow_file: Path, working_dir: Path) -> dict:
        from promptflow._utils.yaml_utils import load_yaml

        flow_data = load_yaml(flow_file)
        # TODO: no outputs definition for eager flow for now
        if flow_data.get("entry", None) is not None:
            return {}

        # TODO: get this from self._get_flow_meta for both eager flow and non-eager flow then remove
        #  dependency on flow_file and working_dir
        from promptflow.contracts.flow import Flow as DataplaneFlow

        dataplane_flow = DataplaneFlow.from_yaml(flow_file, working_dir=working_dir)
        return dataplane_flow.outputs

    @classmethod
    async def create(
        cls,
        flow_file: Path,
        working_dir: Optional[Path] = None,
        *,
        connections: Optional[dict] = None,
        storage: Optional[AbstractRunStorage] = None,
        **kwargs,
    ) -> "CSharpExecutorProxy":
        """Create a new executor"""
        port = kwargs.get("port", None)
        log_path = kwargs.get("log_path", "")
        init_error_file = Path(working_dir) / f"init_error_{str(uuid.uuid4())}.json"
        init_error_file.touch()

        if port is None:
            # if port is not provided, find an available port and start a new execution service
            port = cls.find_available_port()

            process = subprocess.Popen(
                cls._construct_service_startup_command(
                    port=port,
                    log_path=log_path,
                    error_file_path=init_error_file,
                    yaml_path=flow_file.as_posix(),
                )
            )
        else:
            # if port is provided, assume the execution service is already started
            process = None

        outputs_definition = cls.get_outputs_definition(flow_file, working_dir=working_dir)
        chat_output_name = next(
            filter(
                lambda key: outputs_definition[key].is_chat_output,
                outputs_definition.keys(),
            ),
            None,
        )
        executor_proxy = cls(
            process=process,
            port=port,
            working_dir=working_dir,
            # TODO: remove this from the constructor after can always be inferred from flow meta?
            chat_output_name=chat_output_name,
            enable_stream_output=kwargs.get("enable_stream_output", False),
        )
        try:
            await executor_proxy.ensure_executor_startup(init_error_file)
        finally:
            Path(init_error_file).unlink()
        return executor_proxy

    async def destroy(self):
        """Destroy the executor"""
        # process is not None, it means the executor service is started by the current executor proxy
        # and should be terminated when the executor proxy is destroyed if the service is still active
        if self._process and self._is_executor_active():
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()

    def _is_executor_active(self):
        """Check if the process is still running and return False if it has exited"""
        # if prot is provided on creation, assume the execution service is already started and keeps active within
        # the lifetime of current executor proxy
        if self._process is None:
            return True

        # get the exit code of the process by poll() and if it is None, it means the process is still running
        return self._process.poll() is None

    @classmethod
    def find_available_port(cls) -> str:
        """Find an available port on localhost"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("localhost", 0))
            _, port = s.getsockname()
            return str(port)
