# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import json
import os
from datetime import datetime
from enum import Enum
from traceback import TracebackException, format_tb
from types import TracebackType, FrameType

from promptflow.exceptions import PromptflowException, SystemErrorException, UserErrorException, ValidationException

ADDITIONAL_INFO_USER_EXECUTION_ERROR = "ToolExecutionErrorDetails"
ADDITIONAL_INFO_USER_CODE_STACKTRACE = "UserCodeStackTrace"

CAUSE_MESSAGE = "\nThe above exception was the direct cause of the following exception:\n\n"
CONTEXT_MESSAGE = "\nDuring handling of the above exception, another exception occurred:\n\n"
TRACEBACK_MESSAGE = "Traceback (most recent call last):\n"


class RootErrorCode:
    USER_ERROR = "UserError"
    SYSTEM_ERROR = "SystemError"


class ResponseCode(str, Enum):
    SUCCESS = "200"
    ACCEPTED = "202"
    REDIRECTION = "300"
    CLIENT_ERROR = "400"
    SERVICE_ERROR = "500"
    UNKNOWN = "0"


class ErrorResponse:
    """A class that represents the response body when an error occurs.

    It follows the following specification:
    https://github.com/microsoft/api-guidelines/blob/vNext/Guidelines.md#7102-error-condition-responses
    """

    def __init__(self, error_dict):
        self._error_dict = error_dict

    @staticmethod
    def from_error_dict(error_dict):
        """Create an ErrorResponse from an error dict.

        The error dict which usually is generated by ExceptionPresenter.create(exception).to_dict()
        """
        return ErrorResponse(error_dict)

    @staticmethod
    def from_exception(ex: Exception, *, include_debug_info=False):
        presenter = ExceptionPresenter.create(ex)
        error_dict = presenter.to_dict(include_debug_info=include_debug_info)
        return ErrorResponse(error_dict)

    @property
    def message(self):
        return self._error_dict.get("message", "")

    @property
    def response_code(self):
        """Given the error code, return the corresponding http response code."""
        root_error_code = self._error_dict.get("code")
        return ResponseCode.CLIENT_ERROR if root_error_code == RootErrorCode.USER_ERROR else ResponseCode.SERVICE_ERROR

    @property
    def additional_info(self):
        """Return the additional info of the error.

        The additional info is defined in the error response.
        It is stored as a list of dict, each of which contains a "type" and "info" field.

        We change the list of dict to a dict of dict for easier access.
        """
        result = {}
        list_of_dict = self._error_dict.get("additionalInfo")
        if not list_of_dict or not isinstance(list_of_dict, list):
            return result

        for item in list_of_dict:
            # We just ignore the item if it is not a dict or does not contain the required fields.
            if not isinstance(item, dict):
                continue

            name = item.get("type")
            info = item.get("info")
            if not name or not info:
                continue

            result[name] = info

        return result

    def get_additional_info(self, name):
        """Get the additional info by name."""
        return self.additional_info.get(name)

    def get_user_execution_error_info(self):
        """Get user tool execution error info from additional info."""
        user_execution_error_info = self.get_additional_info(ADDITIONAL_INFO_USER_EXECUTION_ERROR)
        if not user_execution_error_info or not isinstance(user_execution_error_info, dict):
            return {}
        return user_execution_error_info

    def to_dict(self):
        from promptflow._core.operation_context import OperationContext

        return {
            "error": self._error_dict,
            "correlation": None,  # TODO: to be implemented
            "environment": None,  # TODO: to be implemented
            "location": None,  # TODO: to be implemented
            "componentName": OperationContext.get_instance().get_user_agent(),
            "time": datetime.utcnow().isoformat(),
        }

    def to_simplified_dict(self):
        return {
            "error": {
                "code": self._error_dict.get("code"),
                "message": self._error_dict.get("message"),
            }
        }

    @property
    def error_codes(self):
        error = self._error_dict
        error_codes = []
        while error is not None:
            code = error.get("code")
            if code is not None:
                error_codes.append(code)
                error = error.get("innerError")
            else:
                break

        return error_codes

    @property
    def error_code_hierarchy(self):
        """Get the code hierarchy from error dict."""

        return "/".join(self.error_codes)

    @property
    def innermost_error_code(self):
        error_codes = self.error_codes

        if error_codes:
            return error_codes[-1]

        return None


class ExceptionPresenter:
    """A class that can extract information from the exception instance.

    It is designed to work for both PromptflowException and other exceptions.
    """

    def __init__(self, ex: Exception):
        self._ex = ex

    @staticmethod
    def create(ex: Exception):
        if isinstance(ex, PromptflowException):
            return PromptflowExceptionPresenter(ex)
        return ExceptionPresenter(ex)

    @property
    def formatted_traceback(self):
        te = TracebackException.from_exception(self._ex)
        return "".join(te.format())

    @property
    def debug_info(self):
        return self.build_debug_info(self._ex)

    def build_debug_info(self, ex: Exception):
        inner_exception: dict = None
        stack_trace = TRACEBACK_MESSAGE + "".join(format_tb(ex.__traceback__))

        if ex.__cause__ is not None:
            inner_exception = self.build_debug_info(ex.__cause__)
            stack_trace = CAUSE_MESSAGE + stack_trace

        elif ex.__context__ is not None and not ex.__suppress_context__:
            inner_exception = self.build_debug_info(ex.__context__)
            stack_trace = CONTEXT_MESSAGE + stack_trace

        return {
            "type": ex.__class__.__qualname__,
            "message": str(ex),
            "stackTrace": stack_trace,
            "innerException": inner_exception,
        }

    @property
    def error_codes(self):
        """The hierarchy of the error codes.

        We follow the "Microsoft REST API Guidelines" to define error codes in a hierarchy style.
        See the below link for details:
        https://github.com/microsoft/api-guidelines/blob/vNext/Guidelines.md#7102-error-condition-responses

        This method returns the error codes in a list. It will be converted into a nested json format by
        error_code_recursed.
        """
        return [infer_error_code_from_class(SystemErrorException), self._ex.__class__.__name__]

    @property
    def error_code_recursed(self):
        """Returns a dict of the error codes for this exception.

        It is populated in a recursive manner, using the source from `error_codes` property.
        i.e. For PromptflowException, such as ToolExcutionError which inherits from UserErrorException,
        The result would be:

          {
            "code": "UserError",
            "innerError": {
              "code": "ToolExecutionError",
              "innerError": None,
            },
          }

        For other exception types, such as ValueError, the result would be:

          {
            "code": "SystemError",
            "innerError": {
              "code": "ValueError",
              "innerError": None,
            },
          }
        """
        current_error = None
        reversed_error_codes = reversed(self.error_codes) if self.error_codes else []
        for code in reversed_error_codes:
            current_error = {
                "code": code,
                "innerError": current_error,
            }

        return current_error

    def to_dict(self, *, include_debug_info=False):
        """Return a dict representation of the exception.

        This dict specification corresponds to the specification of the Microsoft API Guidelines:
        https://github.com/microsoft/api-guidelines/blob/vNext/Guidelines.md#7102-error-condition-responses

        Note that this dict represents the "error" field in the response body of the API.
        The whole error response is then populated in another place outside of this class.
        """
        if isinstance(self._ex, JsonSerializedPromptflowException):
            return self._ex.to_dict(include_debug_info=include_debug_info)

        # Otherwise, return general dict representation of the exception.
        result = {"message": str(self._ex), "messageFormat": "", "messageParameters": {}}
        result.update(self.error_code_recursed)

        if include_debug_info:
            result["debugInfo"] = self.debug_info

        return result


class PromptflowExceptionPresenter(ExceptionPresenter):
    @property
    def error_codes(self):
        """The hierarchy of the error codes.

        We follow the "Microsoft REST API Guidelines" to define error codes in a hierarchy style.
        See the below link for details:
        https://github.com/microsoft/api-guidelines/blob/vNext/Guidelines.md#7102-error-condition-responses

        For subclass of PromptflowException, use the ex.error_codes directly.

        For PromptflowException (not a subclass), the ex.error_code is None.
        The result should be:
        ["SystemError", {inner_exception type name if exist}]
        """
        if self._ex.error_codes:
            return self._ex.error_codes

        # For PromptflowException (not a subclass), the ex.error_code is None.
        # Handle this case specifically.
        error_codes = [infer_error_code_from_class(SystemErrorException)]
        if self._ex.inner_exception:
            error_codes.append(infer_error_code_from_class(self._ex.inner_exception.__class__))
        return error_codes

    def to_dict(self, *, include_debug_info=False):
        result = {
            "message": self._ex.message,
            "messageFormat": self._ex.message_format,
            "messageParameters": self._ex.serializable_message_parameters,
            "referenceCode": self._ex.reference_code,
        }

        result.update(self.error_code_recursed)
        if self._ex.additional_info:
            result["additionalInfo"] = [{"type": k, "info": v} for k, v in self._ex.additional_info.items()]
        if include_debug_info:
            result["debugInfo"] = self.debug_info

        return result


class JsonSerializedPromptflowException(Exception):
    """Json serialized PromptflowException.
    This exception only has one argument message to avoid the
    argument missing error when load/dump with pickle in multiprocessing.
    Ref: https://bugs.python.org/issue32696

    :param message: A Json serialized message describing the error.
    :type message: str
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return self.message

    def to_dict(self, *, include_debug_info=False):
        # Return a dict representation of the inner exception.
        error_dict = json.loads(self.message)

        # The original serialized error might contain debugInfo.
        # We pop it out if include_debug_info is set to False.
        if not include_debug_info and "debugInfo" in error_dict:
            error_dict.pop("debugInfo")

        return error_dict


def get_tb_next(tb: TracebackType, next_cnt: int):
    """Return the nth tb_next of input tb.

    If the tb does not have n tb_next, return the last tb which has a value.
    n = next_cnt
    """
    while tb.tb_next and next_cnt > 0:
        tb = tb.tb_next
        next_cnt -= 1
    return tb


def last_frame_info(ex: Exception):
    """Return the line number where the error occurred."""
    if ex:
        tb = TracebackException.from_exception(ex)
        last_frame = tb.stack[-1] if tb.stack else None
        if last_frame:
            return {
                "filename": last_frame.filename,
                "lineno": last_frame.lineno,
                "name": last_frame.name,
            }

    return {}


def infer_error_code_from_class(cls):
    # Python has a built-in SystemError
    if cls == SystemErrorException:
        return RootErrorCode.SYSTEM_ERROR

    if cls == UserErrorException:
        return RootErrorCode.USER_ERROR
    if cls == ValidationException:
        return "ValidationError"

    return cls.__name__


def is_pf_core_frame(frame: FrameType):
    """Check if the frame is from promptflow core code."""
    from promptflow import _core
    folder_of_core = os.path.dirname(_core.__file__)
    return folder_of_core in frame.f_code.co_filename
