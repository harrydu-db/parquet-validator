import os
import pyarrow.parquet as pq
import pyarrow as pa
from typing import Dict, List, Optional, Set, Tuple
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ParquetValidator:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.schema: Optional[pa.Schema] = None
        self.partition_columns: Set[str] = set()
        self.partition_values: Dict[str, Set[str]] = {}
        self.errors: List[str] = []
        self.directory_structures: Dict[str, Set[Tuple[str, ...]]] = {}

    def validate(self) -> bool:
        """Main validation method that checks folder structure and parquet files."""
        try:
            if not self.base_path.exists():
                self.errors.append(f"Base path {self.base_path} does not exist")
                return False

            # First pass: collect partition columns and validate structure
            self._validate_folder_structure(self.base_path)
            
            # Check for conflicting directory structures
            self._check_conflicting_structures()
            
            # Second pass: validate parquet files and schema
            self._validate_parquet_files(self.base_path)

            if self.errors:
                logger.error("Validation failed with the following errors:")
                for error in self.errors:
                    logger.error(f"- {error}")
                return False
            
            logger.info("Validation completed successfully!")
            return True

        except Exception as e:
            self.errors.append(f"Unexpected error during validation: {str(e)}")
            return False

    def _validate_folder_structure(self, path: Path, current_structure: Tuple[str, ...] = ()) -> None:
        """Recursively validate folder structure and collect partition information."""
        for item in path.iterdir():
            if item.is_dir():
                new_structure = current_structure + (item.name,)
                
                # Track directory structure for each level
                level = len(new_structure)
                if level not in self.directory_structures:
                    self.directory_structures[level] = set()
                self.directory_structures[level].add(new_structure)
                
                # Check if directory name follows partition format (key=value)
                if "=" in item.name:
                    partition_key, partition_value = item.name.split("=", 1)
                    self.partition_columns.add(partition_key)
                    if partition_key not in self.partition_values:
                        self.partition_values[partition_key] = set()
                    self.partition_values[partition_key].add(partition_value)
                self._validate_folder_structure(item, new_structure)

    def _check_conflicting_structures(self) -> None:
        """Check for conflicting directory structures at each level."""
        # First, check if we have multiple root paths that could cause conflicts
        root_paths = set()
        for level, structures in self.directory_structures.items():
            for structure in structures:
                # Get the root path (first directory in the structure)
                if structure:
                    root_paths.add(structure[0])
        
        # If we have multiple root paths, check if they are all partition paths
        # (i.e., they all contain '=' in their name)
        if len(root_paths) > 1:
            all_partition_paths = all("=" in path for path in root_paths)
            if not all_partition_paths:
                self.errors.append(
                    "[CONFLICTING_DIRECTORY_STRUCTURES] Conflicting directory structures detected.\n"
                    "Suspicious paths:\n"
                )
                for root_path in root_paths:
                    path = str(self.base_path / root_path)
                    self.errors.append(f"\t{path}")
                
                self.errors.append(
                    "\nIf provided paths are partition directories, please set 'basePath' in the options "
                    "of the data source to specify the root directory of the table.\n"
                    "If there are multiple root directories, please load them separately and then union them."
                )
                return

        # If no root path conflicts, check for partition key conflicts at each level
        for level, structures in self.directory_structures.items():
            # Get all partition keys at this level
            partition_keys = set()
            for structure in structures:
                for dir_name in structure:
                    if "=" in dir_name:
                        key, _ = dir_name.split("=", 1)
                        partition_keys.add(key)
            
            # If we have multiple different partition keys at the same level, that's a conflict
            if len(partition_keys) > 1:
                self.errors.append(
                    "[CONFLICTING_DIRECTORY_STRUCTURES] Conflicting directory structures detected.\n"
                    "Suspicious paths:\n"
                )
                for structure in structures:
                    path = str(self.base_path / '/'.join(structure))
                    self.errors.append(f"\t{path}")
                
                self.errors.append(
                    "\nIf provided paths are partition directories, please set 'basePath' in the options "
                    "of the data source to specify the root directory of the table.\n"
                    "If there are multiple root directories, please load them separately and then union them."
                )
                break

    def _validate_parquet_files(self, path: Path) -> None:
        """Recursively validate parquet files and their schemas."""
        for item in path.iterdir():
            if item.is_dir():
                self._validate_parquet_files(item)
            elif item.suffix == '.parquet':
                try:
                    # Read only metadata to be efficient
                    parquet_file = pq.ParquetFile(item)
                    
                    # Check schema consistency
                    if self.schema is None:
                        self.schema = parquet_file.schema
                    elif self.schema != parquet_file.schema:
                        self.errors.append(
                            f"Schema mismatch in {item}. Expected schema: {self.schema}, "
                            f"Found schema: {parquet_file.schema}"
                        )
                    
                    # Validate partition values match file metadata
                    file_partitions = parquet_file.metadata.row_group(0).column(0).file_path.split('/')
                    for partition in file_partitions:
                        if "=" in partition:
                            key, value = partition.split("=", 1)
                            if key in self.partition_values and value not in self.partition_values[key]:
                                self.errors.append(
                                    f"Partition value mismatch in {item}. "
                                    f"Value {value} not found in partition {key}"
                                )

                except Exception as e:
                    self.errors.append(f"Error reading parquet file {item}: {str(e)}")

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Validate parquet folder structure and files')
    parser.add_argument('path', help='Path to the parquet folder to validate')
    args = parser.parse_args()

    validator = ParquetValidator(args.path)
    success = validator.validate()
    
    if not success:
        exit(1)

if __name__ == "__main__":
    main() 