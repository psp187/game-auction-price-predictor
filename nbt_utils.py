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

def nbt_to_python(value: Any) -> Any: #What we see in actual json
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


data = """H4sIAAAAAAAA/02Sz4oTQRDGa7JRk1FcEFlykhZcBEMkMyabxFvMJpuFJAvJelFEajKVmSbzj56e1Vz1pC+gLyDjyaNXYZ5EPPoQYncimEM33b+vvurqrjYBqmBwEwCMEpS4a7ww4NogziJpmHAg0TPg5vPIEYRrdAIyDqA65i6NAvRSZfpjwg2Xp0mAmyqUJ7GgiqJ34HaRd4q8O8ErZPPY1UyRUwzRo6esyJd1q9OGu4otpKDIk/5/ek9TQjZQh8pMEBv4GC3/2U6Ooab0EU99HnlskRC5WsF6q7l1Xoo48TdMB+wZsW61j+FQrbov1fj1+Z2aX6myjoq89/P7NzaO47VWizyYXcyGW/7763s24RHt85qO//GBLXi0JrGvADzSx2OwZjJWgq3u7W7UosMjJn2dxX6Gy/UblCTg4Xa3iTMdoOIxSYINS1DIVNulz1MmYvexSst2b3mpEZcUsiVGzCEmaBULj9z722JVGfP+fMhG54vx+eyMzS9OK1CeYUhwS8lT9MJdK0w4HL6VAvtSCu5kktKKbjxUp/2zaf/1zpdlijywn2DXcpxmw+qtsNHqNbuNru20G2RbFrpuu7XqtMtQlTykVGKYqE/0qfbl4wlACa7vmq3qh79XlYIGZQIAAA=="""

export_to_json(data, 'test_file5')