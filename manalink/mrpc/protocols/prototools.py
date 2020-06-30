"""
.. module:: protocols.prototools
    :platforms: Unix
    :synopsis: General tools to aid in protocol development

.. moduleauthor:: Graham Keenan 2020

"""

import json
from typing import (
    Dict, Any, ByteString, Optional, Union, List
)

import time
import socket
from enum import Enum
from dataclasses import dataclass


class ProtoPort(Enum):
    """Enum to hold Protocol Ports.
    Each new protocol will add it's designated port here.

    Inherits:
        Enum: Base Enum class
    """
    UNUSED = 64000  # Defualt -- Unused
    DISCOVERY = 9000  # Discovery Protocol


class ProtoErrorType(Enum):
    """Enum for determine error types in ProtoMessages

    Inherits:
        Enum: Base Enum class
    """

    NONE = -1  # No Error
    MESSAGING = 0  # Error in processing received requests
    CONNECTION = 1  # Connection error between nodes
    EXECUTION = 2  # Error in executing the requested function


@dataclass
class ProtoError:
    """Dataclass for holding error types and messages

    Args:
        message (Optional[str]): Error message
        error_type (Optional[ProtoErrorType]): Type of error
    """

    message: Optional[str] = ''
    error_type: Optional[ProtoErrorType] = ProtoErrorType.NONE

    @classmethod
    def from_dict(cls, fields: Dict):
        """Creates a ProtoError from a dictionary

        Args:
            fields (Dict): ProtoError fields

        Returns:
            ProtoError: New ProtoError class
        """

        error_type = ProtoErrorType(fields['error_type'])
        return cls(message=fields['message'], error_type=error_type)

    @property
    def as_dict(self) -> Dict:
        """Return ProtoError as a dictionary

        Returns:
            Dict: ProtoError fields
        """

        return {
            'message': self.message,
            'error_type': self.error_type.value
        }


class NodeAddress:
    """Class representing a host/port pair

    Args:
        host (Optional[str]): Host address. Defaults to ''.
        port (Optional[int]): Port number. Defaults to -1.
    """

    def __init__(
        self, host: Optional[str] = '', port: Union[ProtoPort, int] = -1
    ):
        self.host = host
        self.port = port.value if isinstance(port, ProtoPort) else port

    def __repr__(self) -> str:
        """Human-readable NodeAddress

        Returns:
            str: NodeAddress info as a string
        """

        return f'NodeAddress(host={self.host}, port={self.port})'

    def set_port(self, new_port: Union[ProtoPort, int]):
        """Set the port to a new value

        Args:
            new_port (int): New port value
        """

        self.port = (
            new_port.value if isinstance(new_port, ProtoPort) else new_port
        )

    @property
    def as_dict(self) -> Dict:
        """NodeAddress represented as a Dict

        Returns:
            Dict: Host/Port information
        """

        return self.__dict__

    @classmethod
    def from_dict(cls, info: Dict):
        """Build a NodeAddress from a dict

        Args:
            info (Dict): Dict info

        Returns:
            NodeAddress: New NodeAddress
        """

        if isinstance(info, dict):
            return cls(**info)

        if isinstance(info, NodeAddress):
            return info


class ProtoResult:
    def __init__(
        self,
        status: Optional[bool] = True,
        results: Optional[List[Any]] = [],
        errors: Optional[Any] = None
    ):
        """Struct to hold the status, results, and erros of any Protocol
        method calls to send back to another Node

        Args:
            status (Optional[bool], optional): Success of the operation.
                                                Defaults to True.

            results (Optional[List[Any]], optional): Any results from the method
                        call to send back to the node. Defaults to [].

            errors (Optional[Any], optional): Errors encountered, if any.
                                                Defaults to None.
        """

        self.status = status
        self.results = results
        self.errors = errors if errors is not None else ProtoError()

    def __repr__(self) -> str:
        """Human-readable ProtoResult

        Returns:
            str: ProtoResult information in string format
        """

        return (
            f'ProtoResult(status={self.status} results={self.results} errors:\
 {self.errors}'
        )

    @ property
    def as_dict(self) -> Dict:
        """ProtoResult as a dictionary

        Returns:
            Dict: ProtoResult fields as a dictionary
        """

        return {
            'status': self.status,
            'results': _ensure_results_serializable(self.results),
            'errors': (
                self.errors.as_dict if self.errors is not None else self.errors
            )
        }

    @ classmethod
    def from_dict(cls, fields: Dict):
        """Create a ProtoResult from a dictionary

        Args:
            fields (Dict): ProtoResult fields

        Returns:
            ProtoResult: New ProtoResult class
        """

        fields['errors'] = (
            ProtoError.from_dict(fields['errors'])
            if fields['errors'] is not None else fields['errors']
        )

        return cls(**fields)

    @ classmethod
    def error_result(cls, err_msg: str, err_type: ProtoErrorType):
        """Create an empty ProtoResult with an error supplied

        Args:
            err_msg (str): Error message to send
            err_type (ProtoErrorType): Type of error encountered

        Returns:
            ProtoResult: New ProtoResult class
        """

        error = ProtoError(
            message=err_msg, error_type=err_type
        )

        return cls(status=False, results=[], errors=error)


class ProtoMessage:
    """Class representing as Protocol Message sent between two nodes.

    Args:
        protocol (Optional[str]): Name of the Protocol. Defaults to None.

        timestamp (Optional[str]): Timestamp when message was created.
                                    Defaults to None.

        function (Optional[str]): Name of protocol function to call.
                                    Defaults to None.

        args (Optional[List[Any]]): Arguments for the protocol function.
                                    Defaults to [].

        status (Optional[bool]): Result of the function call. Defaults to False.

        errors (Optional[Dict]): Error struct listing error code and message.
                                    Defaults to None.

    """

    def __init__(
        self,
        protocol: Optional[str] = None,
        timestamp: Optional[str] = None,
        function: Optional[str] = None,
        args: Optional[List[Any]] = [],
        results: Optional[ProtoResult] = None,
    ):
        self.protocol = protocol
        self.timestamp = str(time.time())
        self.function = function
        self.args = args
        self.results = results if results is not None else ProtoResult()

    def __repr__(self) -> str:
        """Human-readable ProtoMessage

        Returns:
            str: ProtoMessage information in string format
        """

        return (
            f'ProtoMessage(protocol={self.protocol} timestamp={self.timestamp}\
 function={self.function} args={self.args} results={self.results}'
        )

    @ classmethod
    def from_json(cls, json_string: Union[Any]):
        """Create a ProtoMessage from a JSON `string`

        Args:
            json_string (Union[Any]): JSON data. Could be a ByteString,
                                    Dict or String.

        Returns:
            Optional[ProtoMessage]: ProtoMessage if successful, None otherwise
        """
        if not json_string:
            return None

        # Bytes - decode, convert to dict, and build
        if isinstance(json_string, bytes):
            proto_msg = _instantiate_proto_message_dict(
                load_json_string(json_string.decode())
            )

            return cls(**proto_msg)

        # String - convert to dict and build
        if isinstance(json_string, str):
            proto_msg = _instantiate_proto_message_dict(
                load_json_string(json_string)
            )

            return cls(**proto_msg)

        # Dict - build
        if isinstance(json_string, dict):
            proto_msg = _instantiate_proto_message_dict(json_string)
            return cls(**proto_msg)

        # Neither
        return None

    @ property
    def as_encoded_string(self) -> ByteString:
        """Return representation of the ProtoMessage as an encoded string.

        Returns:
            ByteString: ProtoMessage as an encoded string
        """

        return self.as_string.encode()

    @ property
    def as_string(self) -> str:
        """Representation of the ProtoMessage as a JSON string

        Returns:
            str: String representation of ProtoMessage
        """

        proto_msg = self.__dict__
        proto_msg['args'] = _ensure_results_serializable(proto_msg['args'])
        proto_msg['results'] = (
            self.results.as_dict if self.results is not None else self.results
        )

        return json.dumps(proto_msg)

    @ property
    def as_dict(self) -> Dict:
        """Dictionary representation of the ProtoMessage

        Returns:
            Dict: ProtoMessage dict
        """
        msg = {**self.__dict__}
        result = msg['results']

        msg['results'] = (
            result.as_dict if hasattr(result, 'as_dict') else result
        )

        return msg


def _instantiate_proto_message_dict(proto_dict: Dict) -> Dict:
    """Reinstantiates all classes present in the fields of a ProtoMessage.

    Args:
        proto_dict (Dict): ProtoMessage dict

    Returns:
        Dict: ProtoMessage dict with objects all instantiated
    """

    # Results is the only one that needs to be dealt with
    results = proto_dict['results']
    proto_dict['results'] = (
        ProtoResult.from_dict(results) if results is not None else results
    )

    return proto_dict


def encode_dict_as_json(msg: Dict) -> ByteString:
    """Converts dict to string and encodes

    Args:
        msg (Dict): Dict to encode

    Returns:
        ByteString: Encoded dict
    """

    return json.dumps(msg).encode()


def load_json_string(msg: str) -> Optional[str]:
    """Converts a JSON string into a dict

    Args:
        msg (str): JSON string

    Returns:
        Optional[str]: Dict if successful, None otherwise
    """

    try:
        return json.loads(msg)
    except json.JSONDecodeError:
        return None


def _ensure_serializable(target_obj: Any) -> Any:
    """Ensures that whatever object is given is converted to a format that can
    be JSON serialisable

    Args:
        target_obj (Any): Object to convert

    Returns:
        Any: Serialisable format
    """

    # In this case, the object should hyave an `as_dict` method as this is
    # mainly used by the Proto classes
    return target_obj.as_dict if hasattr(target_obj, 'as_dict') else target_obj


def _ensure_results_serializable(results: List[Any]) -> List[Any]:
    """Ensures that the results in ProtoResults are all JSON serialisable

    Args:
        results (List[Any]): List of results

    Returns:
        List[Any]: List of serialisable results
    """

    # List to hold new results
    new_results = []

    # Iterate through each result
    for result in results:
        # Result is a list
        if isinstance(result, list):
            # Ensure each item in the list is serialisable
            new_results.append([
                _ensure_serializable(res) for res in result
            ])

        # Result is a dictionary
        elif isinstance(result, dict):
            # Ensure each value of the dictionary is serialisable
            new_results.append({
                k: _ensure_serializable(v) for k, v in result.items()
            })

        # Result is another type
        else:
            # Make sure it is serialisable
            new_results.append(
                _ensure_serializable(result)
            )

    return new_results

def address_in_use(host: str, port: int) -> bool:
    """Determines if an address is already in use by a socket

    Args:
        host (str): Address
        port (int): Port

    Returns:
        bool: In use or not
    """

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((host, port))
        s.close()
        return False
    except OSError:
        return True
