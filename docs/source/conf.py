from __future__ import annotations

import os
import sys
from datetime import date

# Add package to path for autodoc (optional)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))

project = "ECB–ESMA Securitisation Data Processing Pipeline"
author = "Repository authors"
copyright = f"{date.today().year}, {author}"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "myst_parser",
]

templates_path = ["_templates"]
exclude_patterns = []

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
