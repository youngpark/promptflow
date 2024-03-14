# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------
import hashlib
import os
from os import PathLike
from pathlib import Path
from typing import Tuple, Union

from promptflow._sdk._constants import DAG_FILE_NAME, DEFAULT_ENCODING
from promptflow._utils.logger_utils import LoggerFactory
from promptflow._utils.yaml_utils import dump_yaml, load_yaml

logger = LoggerFactory.get_logger(name=__name__)


def get_flow_lineage_id(flow_dir: Union[str, PathLike]):
    """
    Get the lineage id for flow. The flow lineage id will be same for same flow in same GIT repo or device.
    If the flow locates in GIT repo:
        use Repo name + relative path to flow_dir as session id
    Otherwise:
        use device id + absolute path to flow_dir as session id
    :param flow_dir: flow directory
    """
    flow_dir = Path(flow_dir).resolve()
    if not flow_dir.is_dir():
        flow_dir = flow_dir.parent
    try:
        from git import Repo

        repo = Repo(flow_dir, search_parent_directories=True)
        lineage_id = f"{os.path.basename(repo.working_dir)}/{flow_dir.relative_to(repo.working_dir).as_posix()}"
        logger.debug("Got lineage id %s from git repo.", lineage_id)

    except Exception:
        # failed to get repo, use device id + absolute path to flow_dir as session id
        import uuid

        device_id = uuid.getnode()
        lineage_id = f"{device_id}/{flow_dir.absolute().as_posix()}"
        logger.debug("Got lineage id %s from local since failed to get git info.", lineage_id)

    # hash the value to avoid it gets too long, and it's not user visible.
    lineage_id = hashlib.sha256(lineage_id.encode()).hexdigest()
    return lineage_id


def resolve_flow_path(
    flow_path: Union[str, Path, PathLike], base_path: Union[str, Path, PathLike, None] = None
) -> Tuple[Path, str]:
    """Resolve flow path and return the flow directory path and the file name of the target yaml.

    :param flow_path: The path of the flow directory or the flow yaml file. It can either point to a
      flow directory or a flow yaml file.
    :type flow_path: Union[str, Path, PathLike]
    :param base_path: The base path to resolve the flow path. If not specified, the flow path will be
      resolved based on the current working directory.
    :type base_path: Union[str, Path, PathLike]
    :return: The flow directory path and the file name of the target yaml.
    :rtype: Tuple[Path, str]
    """
    if base_path:
        flow_path = Path(base_path) / flow_path
    else:
        flow_path = Path(flow_path)

    if flow_path.is_dir() and (flow_path / DAG_FILE_NAME).is_file():
        return flow_path, DAG_FILE_NAME
    elif flow_path.is_file():
        return flow_path.parent, flow_path.name

    raise FileNotFoundError(f"Can't find flow with path {flow_path.as_posix()}.")


def load_flow_dag(flow_path: Path):
    """Load flow dag from given flow path."""
    flow_dir, file_name = resolve_flow_path(flow_path)
    flow_path = flow_path / file_name
    if not flow_path.exists():
        raise FileNotFoundError(f"Flow file {flow_path} not found")
    with open(flow_path, "r", encoding=DEFAULT_ENCODING) as f:
        flow_dag = load_yaml(f)
    return flow_path, flow_dag


def dump_flow_dag(flow_dag: dict, flow_path: Path):
    """Dump flow dag to given flow path."""
    flow_dir, flow_filename = resolve_flow_path(flow_path)
    with open(flow_dir / flow_filename, "w", encoding=DEFAULT_ENCODING) as f:
        dump_yaml(flow_dag, f)
    return flow_path
