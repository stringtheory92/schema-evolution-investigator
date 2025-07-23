# Schema Evolution Investigator

A tool for monitoring and analyzing schema changes in data files stored in S3-compatible storage (optimized for Cloudflare R2). This tool scans multiple data sources with YMDH (Year/Month/Day/Hour) directory structures and tracks schema evolution over time.

## Overview

The Schema Evolution Investigator:

- Connects to S3-compatible storage using Cloudflare endpoints
- Scans data files with consistent filenames across YMDH directory structures
- Uses DuckDB to efficiently analyze schema changes
- Tracks when schemas change and what specific columns were added/removed
- Outputs detailed audit reports for further analysis

## Requirements

- Python 3.8+
- S3-compatible storage access (optimized for Cloudflare R2)
- Data organized in YMDH directory structure: `prefix/YYYY/MM/DD/HH/filename`

## Installation

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Create a `.env` file with your configuration:

```env
s3_access_key_id=your_access_key
s3_secret_access_key=your_secret_key
s3_endpoint=https://your-cloudflare-r2-endpoint.com
s3_url_style=path
data_filename=data.parquet
bucket=your-bucket-name
SOURCES_JSON={"src_a":"sources/a","src_b":"sources/b"}
```

## Configuration

### Environment Variables

| Variable               | Description                         | Example                     |
| ---------------------- | ----------------------------------- | --------------------------- |
| `s3_access_key_id`     | S3 access key                       | `your_access_key`           |
| `s3_secret_access_key` | S3 secret key                       | `your_secret_key`           |
| `s3_endpoint`          | S3 endpoint URL                     | `https://your-endpoint.com` |
| `s3_url_style`         | S3 URL style                        | `path` (default)            |
| `data_filename`        | Consistent filename to scan         | `data.parquet` (default)    |
| `bucket`               | S3 bucket name                      | `your-bucket-name`          |
| `SOURCES_JSON`         | JSON mapping of sources to prefixes | See example below           |

### Sources Configuration

The `SOURCES_JSON` should be a JSON string mapping source names to their S3 prefixes:

```json
{
  "src_a": "sources/a",
  "src_b": "sources/b",
  "src_c": "sources/c",
  "src_d": "sources/d",
  "src_e": "sources/e"
}
```

## Usage

### Full Schema Audit

Run the main script to perform a complete schema evolution audit:

```bash
python scripts/main.py
```

This will:

1. Scan all configured data sources
2. Analyze schema for each file found
3. Detect schema changes over time
4. Output results to `schema_evolution_audit.csv`

### Analyze Existing Results

To analyze previously generated audit results:

```bash
python scripts/analysis.py
```

This will read the CSV file and provide detailed information about what columns were added or removed at each schema change.

## Expected Directory Structure

The tool expects data to be organized in YMDH directories:

```
bucket/
└── source-prefix/
    └── 2025/
        └── 03/
            └── 21/
                ├── 00/
                │   └── data.parquet
                ├── 01/
                │   └── data.parquet
                └── ...
```

## Output

The tool generates:

1. **Console output**: Real-time logging and schema change summaries
2. **CSV audit file**: Complete audit results with columns:

   - `source`: Data source name
   - `datetime`: Timestamp of the data file
   - `columns`: List of column names and types
   - `col_hash`: Hash of the schema for change detection
   - `prev_hash`: Previous schema hash
   - `is_change`: Boolean indicating if schema changed
   - `error`: Any errors encountered (if applicable)

3. **Change analysis**: Detailed breakdown of added/removed columns for each schema change

## Features

- **Efficient scanning**: Uses DuckDB for fast schema analysis
- **Error handling**: Gracefully handles missing files and schema errors
- **Change detection**: Identifies exactly when and how schemas change
- **Flexible configuration**: Supports multiple data sources and custom endpoints
- **Detailed logging**: Comprehensive logging with timestamps and progress tracking

## Limitations

- Currently expects Cloudflare R2 or S3-compatible storage
- Assumes YMDH directory structure with consistent filenames
- Date range is currently hardcoded in `main.py` (lines 74-75)

## Troubleshooting

1. **Connection issues**: Verify your S3 credentials and endpoint URL
2. **No files found**: Check your bucket name, prefixes, and date range
3. **Schema errors**: Verify your data files are valid and accessible
4. **JSON parsing errors**: Ensure your `SOURCES_JSON` is valid JSON format
