import time
import json
from dataclasses import dataclass
from .prototools import load_json_string
from typing import Any, Dict, Optional, List, Tuple, Union


@dataclass
class ProtoMessage:
    protocol: Optional[str] = None,
    signature: Optional[str] = None,
    timestamp: float = time.time(),
    function: Optional[str] = None,
    args: Optional[List[Any]] = [],
    result: Optional[bool] = False,
    errors: Optional[str] = None

    @classmethod
    def from_json(cls, json_string: Union[Any]):
        if isinstance(json_string, bytes):
            return cls(
                **load_json_string(
                    json_string.decode()
                )
            )

        if isinstance(json_string, str):
            return cls(**load_json_string(json_string))

        if isinstance(json_string, dict):
            return cls(**json_string)

        return None

    @property
    def as_encoded_string(self):
        return json.dumps(self.__dict__).encode()

    @property
    def as_string(self):
        return json.dumps(self.__dict__)

    @property
    def as_dict(self):
        return self.__dict__


class CoreProtocol:
    @property
    def name(self):
        return self.__class__.__name__

    def create_message(
        self,
        function: Optional[str] = None,
        args: List[Any] = [],
        result: Optional[bool] = False,
        errors: Optional[str] = None
    ):
        return ProtoMessage(
            protocol=self.name,
            signature=self.peer_id,
            function=function,
            args=args,
            result=result,
            errors=errors
        ).as_encoded_string

    def _valid_msg(self, msg: ProtoMessage):
        # Create a dummy message and convert it to Dict
        dummy = ProtoMessage().as_dict
        msg_dict = msg.as_dict

        # Check if all fields in the msg are valid i.e in the dummy message
        return dummy.keys() == msg_dict.keys()

    def _check_msg(self, msg: Dict, original: Any):
        # Malformed request -- Not parsable
        if msg is None:
            return f'Malformed message: {original}'

        # Message is missing fields
        if not self._valid_msg(msg):
            return f'Invalid message fields: {original}'

        return ''

    def parse_request(self, req: Any) -> Dict:
        # Parse the request to get the dictionary
        parsed_req = ProtoMessage.from_json(req)

        errors = self._check_msg(parsed_req, req)
        if errors:
            return self.create_message(
                errors=errors
            )

        # Get function anme and function arguments
        func_name, args = parsed_req.function, parsed_req.args

        # No function by the given name, return response
        if not hasattr(self, func_name):
            return self.create_message(
                function=func_name,
                args=args,
                error=f'No such function: {func_name}'
            )

        # Call the func to get result and errors if present
        func = getattr(self, func_name)

        result, err = func(*args)

        # Return response
        return self.create_message(
            function=func_name, result=result, errors=err
        )

    def parse_response(self, resp: Any) -> Tuple[bool, str]:
        parsed_resp = ProtoMessage.from_json(resp)

        errors = self._check_msg(parsed_resp, resp)
        if errors:
            return self.create_message(errors=errors)

        return parsed_resp
