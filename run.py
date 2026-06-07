import sys
from pathlib import Path

# Run the working tree directly without an install: put the `src/` root on the
# path so `import dacli` resolves to ./src/dacli (the src-layout, see P10).
sys.path.insert(0, str(Path(__file__).parent / "src"))

from dacli.scripts.cli import main

if __name__ == "__main__":
    main()
