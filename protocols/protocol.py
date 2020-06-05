import time
import json
from typing import Any, Dict, Optional

# Valid Request fields
VALID = ['protocol', 'function', 'args', 'timestamp', 'peer']


# TODO::Rethink how to deal with Request/Response messages
class BaseProtocol:
    @property
    def name(self):
        return self.__class__.__name__

    def make_response(
        self,
        protocol: Optional[str] = None,
        peer: Optional[str] = None,
        function: Optional[str] = None,
        result: Optional[bool] = False,
        error: Optional[str] = None,
        **kwargs
    ):
        # Set to protocol name if not set
        protocol = protocol if protocol is not None else self.name

        # Set Peer ID
        peer = peer if peer is not None else self.peer_id

        return {
            'protocol': protocol,
            'peer': peer,
            'function': function,
            'result': result,
            'error': error,
            'timestamp': str(time.time()),
            **kwargs
        }

    def _purify_req(self, req: Any) -> str:
        # Bytes -- decode and convert
        if isinstance(req, bytes):
            req = req.decode()
            return self._req_to_json(req)

        # String -- convert
        if isinstance(req, str):
            return self._req_to_json(req)

        # Just return
        if isinstance(req, dict):
            return req

        # No idea what it is
        return None

    def _req_to_json(self, req: str):
        # Convert string to JSON dictionary
        try:
            return json.loads(str)
        except json.JSONDecodeError:
            return None

    def _valid_req(self, req: Dict):
        # Check all fields are present in request
        for field in req:
            if field not in VALID:
                return False

        return True

    def parse_request(self, req: Any):
        # Parse the request to get the dictionary
        parsed_req = self._purify_req(req)

        # Malformed request -- Not a dict
        if parsed_req is None:
            return self.make_response(
                error=f'Malformed request: {req}'
            )

        # Invalid request -- Missing request fields
        if not self._valid_req(parsed_req):
            return self.make_response(
                error=f'Invalid Request fields: {req}'
            )

        # Get function anme and function arguments
        func_name, args = req['function'], req['args']

        # No function by the given name, return response
        if not hasattr(self, func_name):
            return self.make_response(
                function=func_name,
                error='No such function'
            )

        # Call the func to get result and errors if present
        func = getattr(self, func_name)
        result, err = func(*args)

        # Return response
        return self.make_response(function=func, result=result, error=err)
