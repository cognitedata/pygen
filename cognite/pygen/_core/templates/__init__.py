from pathlib import Path

_template_dir = Path(__file__).resolve().parent

core_api = _template_dir / "core_api.py.jinja"
core_data = _template_dir / "core_data.py.jinja"
type_data = _template_dir / "type_data.py.jinja"
