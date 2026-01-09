# MTBL Extract-Transform Tool
This tool orchestrates the Extract and Transform processes for MTBL data using UV to run each sub-project.

## Usage

### Basic Usage
Run the complete ETL pipeline with defaults:
```bash
uv run mtbl-et
```

### Options
```bash
uv run mtbl-et --help
```

Available options:
- `--year INTEGER` - League year to extract (default: 2025)
- `--force-full-extraction / --no-force-full-extraction` - Force full ESPN extraction, bypassing GraphQL optimization (default: enabled)
- `--extract-output-dir DIRECTORY` - Output directory for extract processes (default: `/Users/Shared/BaseballHQ/resources/extract`)
- `--transform-output-dir DIRECTORY` - Output directory for transform processes (default: `/Users/Shared/BaseballHQ/resources/transform`)

### Examples
```bash
# Run with specific year
uv run mtbl-et --year 2024

# Run without forcing full extraction (use GraphQL optimization)
uv run mtbl-et --no-force-full-extraction

# Run with custom output directories
uv run mtbl-et --extract-output-dir /path/to/extract --transform-output-dir /path/to/transform
```

## Architecture

This orchestrator uses `uv run --directory` to invoke each sub-project, maintaining their independent dependencies:

### Extract Tools
- **ESPN Extractor** - `/Users/Shared/BaseballHQ/tools/_extract/ESPN_API_Extractor`
  - Command: `espn players-extract`
  - Receives: `--year`, `--output-dir`, `--force-full-extraction` (optional)

- **Fangraphs Extractor** - `/Users/Shared/BaseballHQ/tools/_extract/Fangraphs_API_Extractor`
  - Command: `fangraphs-api-extractor`
  - Receives: `--year`, `--output-dir`

- **Baseball Savant Extractor** - `/Users/Shared/BaseballHQ/tools/_extract/Savant_API_Extractor`
  - Command: `savant-extract`
  - Receives: `--season`, `--output-dir`

All extract tools output to `/Users/Shared/BaseballHQ/resources/extract/` by default.

### Transform Tools
- **Player Universe Transformer** - `/Users/Shared/BaseballHQ/tools/_transform/Player_Universe_Trx`
  - Command: `universe_trx`
  - Auto-discovers input files from extract directory
  - Outputs to `/Users/Shared/BaseballHQ/resources/transform/`

## Development

Install dependencies:
```bash
uv sync
```

Run with development dependencies:
```bash
uv sync --dev
```
