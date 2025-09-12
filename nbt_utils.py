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


data = """H4sIAAAAAAAA/11UTW/bRhAdWXIiqR/JJUBzaLto61aCLIX6osQgF1d2ZBeuYkRJDKMNhCU5ogiTu8JyGdvH3HvsISjaojcB/Rn6Kbr0D/RcdJaS3KAAgf16b/bN2xmWAUqQC8sAkNuBndDP7edgdyBToXNlyGse5OCDl8JVyC+5G2EuD6Xj0MenEQ8SIv1Thrt+mMwjflOCwqlUWKTdh/D5ctEbIlds7NHeY7Zc+E7HpqFfaTmOXYVHBDhGHumZOfRq7V6XRqzUOlaVJk6l1qtm8Fpzv9VuVcEiwiFOUSRZOF5r9juNDaf1f45tEaNCjLFWKILtJT1nDWt3N7iW3Wp0q1An5ECFmg1mXHhZfKfW7O6t0TTZwtuN5l4VGlv4IY95sIHbrQ285dzCLbtBXNgn/InQGEVhgJv4bq1tb/BbMc1WJuYzs/qBKKvf3tLs9WbZX/3ys1mSvWSuM1TySs/YK5rumzhnSmr0dCgFe0WQL4n/HP3Uw4TpGTI/U8puZMo0v0Q2VTKGTwh0FdKxSph7YxxqWnu016AAXy8X9tM0itgYNftWijR5zM4zrCuVIGyvYj3qVMHgx3N+JRLG2ToYi0NhdOAbVCYqti3jMBIyQU8KP2HpnGlJhJhfh3Eam/dssls1DfiC5hcyVWwr7yokKVrxNxhlTOHDp4TB63kkfWR0m6BqoyRQYBxiYlK4T7YcCC9EsckAqqYqFRc6WVeQMW717g/23mMaFXNU8JBGMmTANfdk7BpCL6KMIhPZpnJeLqLl4pIvF4qtV99fvDg+GbDDl6Ph0bMROz0aDk9GwzH7D5uHXU9GUsFfPz4pQmHEY4QuHW9FjtBTUnyTsFMMglAE5lJ79euf73+kafX7T1CGe0fX5MeB1ip0U41JHu4pTnncTNJ5oLiPpj2pXe/PpJ7MpeZaTjzT17RdLkMhwDgpwcepiKR3if4kiaROTPPmoPjdwfjs6PnEgiIUY+mH05AcucvXMvPw0eaGSWYIUYpF8+uAB2fPzol2fvLimIatAWX4kIqeGkvHxCad5fltrRJ3Nw93gqyYzYJuLKQpxfrKsfvTvuNYdZ/b03rH5V7dsd1+3eq3vW67703dtleAkg5jTDSP5yT8Xe7vnQuAHbizfksKCP8C9pVrsuEEAAA="""


export_to_json(data, 'test_file3')