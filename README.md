# Parquet Folder Validator

A fast and efficient Python tool to validate parquet folder structures and files.

## Features

- Validates folder structure for partitioned parquet datasets
- Checks parquet file format and schema consistency
- Verifies partition values match file metadata
- Efficient validation by only reading metadata, not the actual data
- Detailed error reporting

## Installation

1. Clone this repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Run the validator on a parquet folder:

```bash
python parquet_validator.py /path/to/parquet/folder
```

The script will:
- Check if the folder structure follows parquet partitioning conventions
- Validate all parquet files are readable
- Ensure schema consistency across all files
- Verify partition values match the folder structure

## Output

- Success: The script will exit with code 0 and print "Validation completed successfully!"
- Failure: The script will exit with code 1 and print detailed error messages

## Example

```bash
python parquet_validator.py sample/
``` 