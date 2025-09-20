from __future__ import annotations
import base64, gzip, io, json, os
from pathlib import Path
from typing import Any
import nbtlib

def decode_nbt(b64: str) -> nbtlib.File:
    raw = base64.b64decode(b64)
    nbt_bytes = gzip.decompress(raw) if raw[:2] == b'\x1f\x8b' else raw

    with io.BytesIO(nbt_bytes) as dec:
        return nbtlib.File.parse(dec)

def nbt_to_python(value: Any) -> Any:
    if hasattr(value, "unpack"):
        return value.unpack()
    if isinstance(value, (list, tuple)):
        return [nbt_to_python(x) for x in value]
    if isinstance(value, dict):
        return {str(k): nbt_to_python(v) for k, v in value.items()}
    return str(value)

def nbt_base64_to_dict(b64: str) -> dict:
    nbt_obj= decode_nbt(b64)
    py_obj = nbt_to_python(nbt_obj)

    return py_obj

def export_to_json(data: str|dict|list, filename: str | os.PathLike) -> Path:
    path = Path(filename)

    if path.suffix.lower() != ".json":
        path = path.with_suffix(".json")
    path.parent.mkdir(parents=True, exist_ok=True)

    if isinstance(data, str):
        payload = nbt_base64_to_dict(data)
    elif isinstance(data, (dict, list)):
        payload = data
    else:
        raise TypeError("Wrong data type, check if str")

    with path.open("w", encoding='utf8') as f:
        json.dump(payload, f, indent=4, ensure_ascii=False)
    return path


data = """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"""

export_to_json(data, 'test_file')