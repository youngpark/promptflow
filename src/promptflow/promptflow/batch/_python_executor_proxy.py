# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from promptflow._core._errors import UnexpectedError
from promptflow._core.operation_context import OperationContext
from promptflow._core.run_tracker import RunTracker
from promptflow._sdk._constants import FLOW_META_JSON_GEN_TIMEOUT
from promptflow._utils.logger_utils import bulk_logger
from promptflow.batch._base_executor_proxy import AbstractExecutorProxy
from promptflow.contracts.run_mode import RunMode
from promptflow.executor import FlowExecutor
from promptflow.executor._line_execution_process_pool import LineExecutionProcessPool
from promptflow.executor._result import AggregationResult, LineResult
from promptflow.executor._script_executor import ScriptExecutor
from promptflow.storage._run_storage import AbstractRunStorage


class PythonExecutorProxy(AbstractExecutorProxy):
    def __init__(self, flow_executor: FlowExecutor):
        self._flow_executor = flow_executor

    @classmethod
    def generate_flow_metadata(
        cls,
        flow_file: Path,
        working_dir: Path,
        dump: bool = True,
        timeout: int = FLOW_META_JSON_GEN_TIMEOUT,
        load_in_subprocess: bool = True,
    ) -> Dict[str, Any]:
        from promptflow import load_flow
        from promptflow._sdk._utils import generate_flow_meta
        from promptflow._sdk.entities._eager_flow import EagerFlow

        flow = load_flow(flow_file)
        if isinstance(flow, EagerFlow):
            # generate flow.json only for eager flow for now
            return generate_flow_meta(
                flow_directory=working_dir,
                source_path=flow.entry_file,
                entry=flow.entry,
                dump=dump,
                timeout=timeout,
                load_in_subprocess=load_in_subprocess,
            )
        else:
            # we will also skip dump for now
            return {}

    @classmethod
    async def create(
        cls,
        flow_file: Path,
        working_dir: Optional[Path] = None,
        *,
        connections: Optional[dict] = None,
        storage: Optional[AbstractRunStorage] = None,
        **kwargs,
    ) -> "PythonExecutorProxy":
        flow_executor = FlowExecutor.create(flow_file, connections, working_dir, storage=storage, raise_ex=False)
        return cls(flow_executor)

    async def exec_aggregation_async(
        self,
        batch_inputs: Mapping[str, Any],
        aggregation_inputs: Mapping[str, Any],
        run_id: Optional[str] = None,
    ) -> AggregationResult:
        with self._flow_executor._run_tracker.node_log_manager:
            return self._flow_executor._exec_aggregation(batch_inputs, aggregation_inputs, run_id=run_id)

    async def _exec_batch(
        self,
        batch_inputs: List[Mapping[str, Any]],
        output_dir: Path,
        run_id: Optional[str] = None,
        batch_timeout_sec: Optional[int] = None,
        line_timeout_sec: Optional[int] = None,
        worker_count: Optional[int] = None,
    ) -> Tuple[List[LineResult], bool]:
        # TODO: Refine the logic here since the script executor actually doesn't have the 'node' concept
        if isinstance(self._flow_executor, ScriptExecutor):
            run_tracker = RunTracker(self._flow_executor._storage)
        else:
            run_tracker = self._flow_executor._run_tracker

        with run_tracker.node_log_manager:
            OperationContext.get_instance().run_mode = RunMode.Batch.name
            if self._flow_executor._flow_file is None:
                raise UnexpectedError(
                    "Unexpected error occurred while init FlowExecutor. Error details: flow file is missing."
                )

            if batch_timeout_sec:
                bulk_logger.info(f"The timeout for the batch run is {batch_timeout_sec} seconds.")

            with LineExecutionProcessPool(
                self._flow_executor,
                len(batch_inputs),
                run_id,
                output_dir,
                batch_timeout_sec=batch_timeout_sec,
                line_timeout_sec=line_timeout_sec,
                worker_count=worker_count,
            ) as pool:
                line_number = [batch_input["line_number"] for batch_input in batch_inputs]
                line_results = await pool.run(zip(line_number, batch_inputs))

            # For bulk run, currently we need to add line results to run_tracker
            self._flow_executor._add_line_results(line_results, run_tracker)
        return line_results, pool.is_timeout

    def get_inputs_definition(self):
        return self._flow_executor.get_inputs_definition()

    @classmethod
    def _get_tool_metadata(cls, flow_file: Path, working_dir: Path) -> dict:
        from promptflow._sdk._utils import generate_flow_tools_json

        return generate_flow_tools_json(
            flow_directory=working_dir,
            dump=False,
            used_packages_only=True,
        )
