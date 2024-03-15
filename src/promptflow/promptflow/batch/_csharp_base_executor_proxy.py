# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, List, Mapping, Optional

import pydash

from promptflow._sdk._constants import ALL_CONNECTION_TYPES
from promptflow._utils.yaml_utils import load_yaml
from promptflow.batch._base_executor_proxy import APIBasedExecutorProxy
from promptflow.executor._result import AggregationResult

EXECUTOR_SERVICE_DLL = "Promptflow.dll"


class CSharpBaseExecutorProxy(APIBasedExecutorProxy):
    """Base class for csharp executor proxy for local and runtime."""

    def __init__(
        self,
        *,
        working_dir: Path = None,
        enable_stream_output: bool = False,
    ):
        super().__init__(working_dir=working_dir, enable_stream_output=enable_stream_output)

    async def exec_aggregation_async(
        self,
        batch_inputs: Mapping[str, Any],
        aggregation_inputs: Mapping[str, Any],
        run_id: Optional[str] = None,
    ) -> AggregationResult:
        # TODO: aggregation is not supported for now?
        return AggregationResult({}, {}, {})

    @classmethod
    def _construct_service_startup_command(
        cls,
        port,
        log_path,
        error_file_path,
        yaml_path: str = "flow.dag.yaml",
        log_level: str = "Warning",
        assembly_folder: str = ".",
    ) -> List[str]:
        return [
            "dotnet",
            EXECUTOR_SERVICE_DLL,
            "--execution_service",
            "--port",
            port,
            "--yaml_path",
            yaml_path,
            "--assembly_folder",
            assembly_folder,
            "--log_path",
            log_path,
            "--log_level",
            log_level,
            "--error_file_path",
            error_file_path,
        ]

    @classmethod
    def get_used_connection_names(cls, flow_file: Path, working_dir: Path) -> List[str]:
        tools_meta = cls.get_tool_metadata(
            flow_file=flow_file,
            working_dir=working_dir,
        )
        flow_dag = load_yaml(flow_file)

        connection_inputs = defaultdict(set)
        for package_id, package_meta in tools_meta.get("package", {}).items():
            for tool_input_key, tool_input_meta in package_meta.get("inputs", {}).items():
                if ALL_CONNECTION_TYPES.intersection(set(tool_input_meta.get("type"))):
                    connection_inputs[package_id].add(tool_input_key)

        connection_names = set()
        # TODO: we assume that all variants are resolved here
        # TODO: only literal connection inputs are supported
        # TODO: check whether we should ask for a specific function in csharp-core
        for node in flow_dag.get("nodes", []):
            package_id = pydash.get(node, "source.tool")
            if package_id in connection_inputs:
                for connection_input in connection_inputs[package_id]:
                    connection_name = pydash.get(node, f"inputs.{connection_input}")
                    if connection_name and not re.match(r"\${.*}", connection_name):
                        connection_names.add(connection_name)
        return list(connection_names)
