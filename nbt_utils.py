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


data = """H4sIAAAAAAAA/2VUTW8jRRAtx/lwvEFZhPhacWhEFsU4yc54MnbiW9aJHQsnu4q9QZys9kxl3GTcbc305OOCtDcu7AkhhHYlTvjAH+DCxT/F/4ETEku1xzFB+OLuqtfdr+q9qTzAKmREHgAyC7Ag/MxWBpZqKpE6k4es5sEqLKL0+mB+GXjwQvYi5Je8F2ImC6vHwsd6yIOYsn/nYcUX8TDkt3SopSLMUXQDPhuPKsfIQ92vsvHIKzp2hf5xs1i2CrTY3yxWCrBBoEO8QBmjQfGiXS6lKOce6hNCtXWEMphf5qZJxy3Ap5StRUKzWp9Lb3rPftF2H6cIWtyDHPIBD2aQXWsGoUUKaUqNYSgCnF3TK9ql2UMleujDeUXsDAmUUrEtKvddAo1H/ngUfiViXw3YOaxRqBGpa0KfN+E92p1wydk5HwxFhBRrwkMKPo+URk8LJQ1snSJn+E1yhZJrAsE7FOj0VSRj1qQTADvjUbmehCFro2ZPlUziKjsXV9wTKompvuhSYhwTs8qm9WS3ABatTPUq1ibqlCa/v2JtlYQXobo2sCFGzI2ZkMxTgx7X8BEFr/siRCNILEl2IQOKVeFjOj95+X2qgDV585LdqWLUnqWwWLImP/+QcmMHWnPvkrWHiD58MAddUNsmr3+dxd15vHKShFoMw1vT/cnrV+y+JCbfm2bsnZJ7Y1oxp1OyrMlvf7A6RsoT2oAqPOBCxpodSR+jAUr4Yo4nn1lT9Sv+1BDsf1gAypafC6RnZ11Oc1yyp0l4TX2Gz+n8l4KkuDvFtGLc85JBEhr1/NTYRuY5Gd3HwQ48osh/7jaUHMedvPkOqpQ7xRvNXgyDiPuz78JxTZKWe5u0dfa2yq5NwCdUu2ttWRbJvFcg1sZSB9ITKGfugALBGhGXU/mpcOPnyU+/sHvfw8wGRl/yilfjpBl5YeqiEK8w3EmNZ/w9Hl3y8Shi6e7k685xs8ZaR41G87TRZv9isrDkqVBF8DZ5m4PFUz5AeEzpO3J1IXnIDjHWtJjav4VBQE6LIQ/rRzc64uSdSPQSjXEW1iNOfG+7SdoUM3hoED3sK90dKs216npmelF4PXf38oOS61Ztu1Kl/yzkkbTrXpJedPjHv3KQGyhfXAiqe4WnpHKwqiJBJDo8gPdrZwf1DlXVbZw1D7u1Z63WUa2TM9MSHtWbpwet7uFRmwAHneaz0+5dC/KwZgYn9ZsMoQ1z4+gB+aF7PR0NRHEpC8t6+lHTJpuFNWLGu1fpZKDQCrGN5lNgdiCYzhLaLFN2OJ8aJgDU4CQhXhu+V0HP5aVt2y9XtnfL9v72vrPnbDvuPpZc9Jy9Mi7CKhGixtN71MVv/xwv0iULsJyawUz8fwBIYBIIIAYAAA=="""

export_to_json(data, 'final_leggings')