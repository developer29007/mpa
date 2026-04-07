import os
import sys

from mcp_server.server import mcp


def main():
    if not os.environ.get("MPA_DSN"):
        print("Error: MPA_DSN environment variable required", file=sys.stderr)
        print("  e.g. MPA_DSN=postgresql://postgres:pass@localhost:5432/mpa", file=sys.stderr)
        sys.exit(1)
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
