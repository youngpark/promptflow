# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

from promptflow.exceptions import ErrorTarget, UserErrorException


class FlowFilePathInvalid(UserErrorException):
    pass


class ExecutionTimeoutError(UserErrorException):
    """Exception raised when execution timeout"""

    def __init__(self, timeout):
        super().__init__(
            message_format="Execution timeout for exceeding {timeout} seconds",
            timeout=timeout,
            target=ErrorTarget.EXECUTOR,
        )


class ExecutionCanceledError(UserErrorException):
    """Exception raised when execution is canceled"""

    def __init__(self, run_id):
        super().__init__(
            message_format="The execution for run {run_id} is canceled.",
            run_id=run_id,
            target=ErrorTarget.EXECUTOR,
        )
