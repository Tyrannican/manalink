import json
from typing import Dict, Any, ByteString, Optional, Union


def encode_dict_as_json(msg: Dict) -> ByteString:
    return json.dumps(msg).encode()


def load_json_string(msg: Union[str, ByteString]) -> Optional[None]:
    try:
        return json.loads(msg)
    except json.JSONDecodeError:
        return None


def convert_msg_to_dict(msg: Any) -> Optional[Dict]:
    if isinstance(msg, bytes):
        return load_json_string(msg.decode())

    if isinstance(msg, str):
        return load_json_string(msg)

    if isinstance(msg, dict):
        return msg

    return None
