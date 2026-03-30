"""Main entry point: python -m pipeline [command]"""

import sys

from pipeline.ingestion.orchestrator import main as orchestrator_main


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m pipeline [ingest|analyze|figures|status]")
        print("  ingest   — run incremental data ingestion")
        print("  analyze  — run all analysis")
        print("  figures  — generate all figures")
        print("  status   — show pipeline watermarks")
        sys.exit(1)

    command = sys.argv[1]
    # Remove the command from argv so subcommands parse correctly
    sys.argv = [sys.argv[0]] + sys.argv[2:]

    if command == "ingest":
        orchestrator_main()
    elif command == "analyze":
        from pipeline.analysis.run_all import main as analyze_main
        analyze_main()
    elif command == "figures":
        from pipeline.figures.generate_all import main as figures_main
        figures_main()
    elif command == "status":
        sys.argv = [sys.argv[0], "--status"]
        orchestrator_main()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
