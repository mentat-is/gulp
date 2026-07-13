"""
This module contains classes and functions for working with JSend responses.
JSend is a specification for building JSON APIs that respond in a consistent format.
For more information, see https://github.com/omniti-labs/jsend.
"""

import orjson
from enum import StrEnum
from pprint import pprint
from typing import Any, Optional

import muty.log
import muty.string
import muty.time
from muty.log import MutyLogger
from pydantic import BaseModel, ConfigDict, Field


class JSendException(Exception):
    """
    Exception class for JSend errors.
    """

    def __init__(
        self,
        message: str = None,
        req_id: str = None,
        status_code: int = 500,
    ):
        """
        Initializes the JSendException.

        Args:
            message (str, optional): The error message. Defaults to None.
            req_id (str, optional): The request ID. Defaults to None.
            status_code (int, optional): The HTTP status code. Defaults to 500.
        """
        self.req_id = req_id
        self.status_code = status_code

        msg: str = ""
        if message:
            msg = message
        else:
            msg = "ERROR!"

        super().__init__(msg)

    def to_string(self, with_full_traceback: bool = False) -> str:
        """
        converts the exception to a string.

        Args:
            with_full_traceback (bool): Whether to include the full traceback. Defaults to False.
        Returns:
            str: The string representation of the exception.
        """

        s = muty.log.exception_to_string(self, with_full_traceback=with_full_traceback)
        return s


class JSendResponseStatus(StrEnum):
    """
    Enum for JSend response status.
    """

    SUCCESS = "success"
    PENDING = "pending"
    ERROR = "error"
    FAIL = "fail"


class JSendResponse(BaseModel):
    """
    Model for responses
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        json_schema_extra={
            "examples": [
                {
                    "status": "success",
                    "timestamp_msec": 1692870496556,
                    "req_id": "the_request_id",
                    "data": {"results": [1, 2, 3]},
                }
            ]
        },
    )

    status: JSendResponseStatus = Field(
        ...,
        description="response status",
    )
    timestamp_msec: int = Field(
        ...,
        description="response timestamp in milliseconds from unix epoch.",
    )
    req_id: str = Field(
        ...,
        description='the same "req_id" that was sent in the request.',
    )
    data: Optional[Any] = Field(
        default=None,
        description="depends on the response, may contain the result or error data.",
    )

    @staticmethod
    def success(req_id: str = None, data: dict = None, print_response: bool = True) -> dict:
        """
        Creates a JSend successful response dictionary.

        Args:
            req_id (str): The request ID to be added to the response.
            data (dict): The data of the response (should contain the result itself, dict layout is API dependent).
            print_response (bool): Whether to print the response. Defaults to True.

        Returns:
            dict: The JSend successful response.
        """
        js = {"status": JSendResponseStatus.SUCCESS.value}
        js["timestamp_msec"] = muty.time.now_msec()
        if req_id:
            js["req_id"] = req_id
        if data is not None:
            js["data"] = data

        if print_response:
            MutyLogger.get_instance().info(
                orjson.dumps(js, option=orjson.OPT_INDENT_2).decode()
            )
        return js

    @staticmethod
    def pending(req_id: str, print_response: bool = True) -> dict:
        """
        Creates a JSend pending response dictionary.

        Args:
            req_id (str): The request ID to be added to the response.
            print_response (bool): Whether to print the response. Defaults to True.

        Returns:
            dict: The JSend pending response.
        """
        js = {"status": JSendResponseStatus.PENDING.value}
        js["timestamp_msec"] = muty.time.now_msec()
        if req_id:
            js["req_id"] = req_id
        if print_response:
            MutyLogger.get_instance().info(
                orjson.dumps(js, option=orjson.OPT_INDENT_2).decode()
            )
        return js

    @staticmethod
    def error(
        req_id: str = None,
        ex: Exception | dict | str = None,
        data: dict = None,
        full_exception_traceback: bool = True,
        print_response: bool = True,
        **kwargs,
    ) -> dict:
        """
        Creates a JSend error response dictionary

        Args:
            req_id (str): The request ID to be added to the response.
            ex (Exception|dict|str): The exception object or error message.
            data (dict): custom data to be added to the response.
            full_exception_traceback (bool): Whether to include the full traceback in the error details. Defaults to False.
            print_response (bool): Whether to print the response. Defaults to True.
            **kwargs: Arbitrary keyword arguments.
        Returns:
            dict: The JSend error response.
        """
        js = {"status": JSendResponseStatus.ERROR.value}
        if req_id:
            js["req_id"] = req_id
        js["timestamp_msec"] = muty.time.now_msec()
        if data:
            js["data"] = data
        else:
            js["data"] = {}

        d = js["data"]

        if ex:
            # print("************ ex=%s" % (ex))
            if isinstance(ex, Exception):
                d["__error"] = {
                    "name": ex.__class__.__name__,
                    "msg": muty.string.make_shorter(str(ex), max_len=1024),
                    "trace": muty.log.exception_to_string(
                        ex, with_full_traceback=full_exception_traceback
                    ),
                }
            elif isinstance(ex, dict):
                d["__error"] = ex
            else:  # str
                d["__error"] = {"msg": ex}

        for k, v in kwargs.items():
            if d.get("__error") is None:
                d["__error"] = {}
            d["__error"][k] = v

        if print_response:
            MutyLogger.get_instance().error(
                orjson.dumps(js, option=orjson.OPT_INDENT_2).decode()
            )
        return js

    @staticmethod
    def check_success(js: dict, pending_is_success: bool = True) -> bool:
        """
        Checks if a JSend response dictionary is successful.

        Args:
            js (dict): The JSend response.
            pending_is_success (bool): Whether pending status should be considered as success. Defaults to True.

        Returns:
            bool: True if the response is successful, False otherwise.
        """
        if not js:
            return False

        res = js.get("status", None)
        if not res:
            return False

        res = str(res).lower()
        if res == JSendResponseStatus.SUCCESS.value:
            return True
        if pending_is_success:
            if res == JSendResponseStatus.PENDING.value:
                return True

        return False
