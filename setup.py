from setuptools import setup, find_packages
from pathlib import Path

# Read README.md
readme_path = Path(__file__).parent / "README.md"
long_description = readme_path.read_text(encoding="utf-8") if readme_path.exists() else ""

# Read requirements.txt
requirements_path = Path(__file__).parent / "requirements.txt"
requirements = []
if requirements_path.exists():
    requirements = [
        line.strip() for line in requirements_path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]

setup(
    name = "dacli",
    version = "0.1.0",
    author = "Mouad Jaouhari",
    author_email = "github@mj-dev.net",
    url = "https://github.com/mouadja02/dacli",
    description = "Your autonomous data engineering CLI agent",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    packages = find_packages(),
    include_package_data = True,
    package_data = {
        "dacli": ["prompts/*.md"]
    },
    python_requires = ">=3.9",
    install_requires = requirements,
    entry_points = {
        "console_scripts": [
            "dacli = scripts.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "Intended Audience :: Data Engineers",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database :: Data Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
    ],
    keywords = "data engineering, autonomous agent, data engineering CLI"
)