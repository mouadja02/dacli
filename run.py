import sys
from pathlib import Path

# Run the working tree directly without an install. dacli is a PEP 420 namespace
# split across four wheels (M13); put each package's `src/` root on the path so
# `import dacli.*` resolves across them.
_here = Path(__file__).parent
for _pkg in ("dacli-ai", "dacli-core", "dacli-tui", "dacli"):
    sys.path.insert(0, str(_here / "packages" / _pkg / "src"))

from dacli.scripts.cli import main

if __name__ == "__main__":
    main()
