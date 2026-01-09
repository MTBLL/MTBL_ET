"""Main orchestrator for MTBL Extract-Transform processes."""

import subprocess
import sys
from pathlib import Path

import click


# Define paths to sub-projects
BASE_TOOLS_PATH = Path("/Users/Shared/BaseballHQ/tools")
EXTRACT_PATH = BASE_TOOLS_PATH / "_extract"
TRANSFORM_PATH = BASE_TOOLS_PATH / "_transform"
RESOURCES_PATH = Path("/Users/Shared/BaseballHQ/resources")

# Extract tools
ESPN_EXTRACTOR = EXTRACT_PATH / "ESPN_API_Extractor"
FANGRAPHS_EXTRACTOR = EXTRACT_PATH / "Fangraphs_API_Extractor"
SAVANT_EXTRACTOR = EXTRACT_PATH / "Savant_API_Extractor"

# Transform tools
PLAYER_UNIVERSE_TRX = TRANSFORM_PATH / "Player_Universe_Trx"

# Output directories (from README)
EXTRACT_OUTPUT_DIR = RESOURCES_PATH / "extract"
TRANSFORM_OUTPUT_DIR = RESOURCES_PATH / "transform"


def run_uv_tool(
    directory: Path,
    command: str,
    args: list[str],
    description: str,
    allow_exit_code_1: bool = False,
) -> None:
    """Run a UV tool from a specific directory with arguments.

    Args:
        directory: Path to the tool's directory
        command: The command to run (script name from pyproject.toml)
        args: Additional command-line arguments to pass
        description: Human-readable description for logging
        allow_exit_code_1: If True, treat exit code 1 as success (for tools with buggy exit codes)
    """
    print(f"\n{'='*80}")
    print(f"Running: {description}")
    print(f"Directory: {directory}")
    print(f"Command: {command} {' '.join(args)}")
    print(f"{'='*80}\n")

    cmd = ["uv", "run", "--directory", str(directory), command] + args

    try:
        subprocess.run(cmd, check=True, capture_output=False)
        print(f"\n✓ {description} completed successfully\n")
    except subprocess.CalledProcessError as e:
        # Some tools (like Player Universe Transformer) return exit code 1 even on success
        if allow_exit_code_1 and e.returncode == 1:
            print(f"\n✓ {description} completed successfully\n")
        else:
            print(
                f"\n✗ {description} failed with exit code {e.returncode}\n",
                file=sys.stderr,
            )
            sys.exit(e.returncode)
    except FileNotFoundError:
        print(
            "\n✗ UV not found. Please install UV first: https://docs.astral.sh/uv/\n",
            file=sys.stderr,
        )
        sys.exit(1)


def run_extract(
    year: int,
    force_full_extraction: bool,
    extract_output_dir: Path,
) -> None:
    """Run all extract processes.

    Args:
        year: League year to extract
        force_full_extraction: Force full ESPN extraction
        extract_output_dir: Directory for extract output
    """
    print("\n" + "="*80)
    print("STARTING EXTRACT PROCESSES")
    print("="*80)

    # ESPN Extractor - players-extract subcommand
    espn_args = [
        "players-extract",
        "--year", str(year),
        "--output-dir", str(extract_output_dir),
    ]
    if force_full_extraction:
        espn_args.append("--force-full-extraction")

    run_uv_tool(ESPN_EXTRACTOR, "espn", espn_args, "ESPN API Extractor")

    # Fangraphs Extractor
    fangraphs_args = [
        "--year", str(year),
        "--output-dir", str(extract_output_dir),
    ]
    run_uv_tool(
        FANGRAPHS_EXTRACTOR,
        "fangraphs-api-extractor",
        fangraphs_args,
        "Fangraphs API Extractor",
    )

    # Savant Extractor
    savant_args = [
        "--season", str(year),
        "--output-dir", str(extract_output_dir),
    ]
    run_uv_tool(SAVANT_EXTRACTOR, "savant-extract", savant_args, "Savant API Extractor")

    print("\n" + "="*80)
    print("EXTRACT PROCESSES COMPLETED")
    print("="*80)


def run_transform() -> None:
    """Run all transform processes.

    Note: Player Universe Transformer auto-discovers input files and outputs
    to the configured transform directory.
    """
    print("\n" + "="*80)
    print("STARTING TRANSFORM PROCESSES")
    print("="*80)

    # Player Universe Transformer - no args needed, uses defaults
    # Note: This tool has a bug where it returns exit code 1 on success
    run_uv_tool(
        PLAYER_UNIVERSE_TRX,
        "universe_trx",
        [],
        "Player Universe Transformer",
        allow_exit_code_1=True,
    )

    print("\n" + "="*80)
    print("TRANSFORM PROCESSES COMPLETED")
    print("="*80)


@click.command()
@click.option(
    "--year",
    default=2025,
    type=int,
    show_default=True,
    help="League year to extract",
)
@click.option(
    "--force-full-extraction/--no-force-full-extraction",
    default=True,
    show_default=True,
    help="Force full ESPN extraction, bypassing GraphQL optimization",
)
@click.option(
    "--extract-output-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    default=EXTRACT_OUTPUT_DIR,
    show_default=True,
    help="Output directory for extract processes",
)
@click.option(
    "--transform-output-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    default=TRANSFORM_OUTPUT_DIR,
    show_default=True,
    help="Output directory for transform processes (informational only)",
)
def main(
    year: int,
    force_full_extraction: bool,
    extract_output_dir: Path,
    transform_output_dir: Path,
) -> None:
    """MTBL Extract-Transform orchestrator.

    Runs the complete ETL pipeline for MTBL data:
    1. Extract from ESPN, Fangraphs, and Savant APIs
    2. Transform into player universe and league data
    """
    print("\n" + "="*80)
    print("MTBL EXTRACT-TRANSFORM ORCHESTRATOR")
    print("="*80)
    print(f"Year: {year}")
    print(f"Force Full Extraction: {force_full_extraction}")
    print(f"Extract Output: {extract_output_dir}")
    print(f"Transform Output: {transform_output_dir}")
    print("="*80)

    try:
        # Run extract processes
        run_extract(year, force_full_extraction, extract_output_dir)

        # Run transform processes
        run_transform()

        print("\n" + "="*80)
        print("ALL PROCESSES COMPLETED SUCCESSFULLY")
        print("="*80 + "\n")

    except KeyboardInterrupt:
        print("\n\n✗ Process interrupted by user\n", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}\n", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
