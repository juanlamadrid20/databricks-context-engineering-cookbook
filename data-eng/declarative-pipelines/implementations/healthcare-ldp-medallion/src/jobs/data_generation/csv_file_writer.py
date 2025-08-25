"""
CSV File Writer for Healthcare Data Generation

Write synthetic healthcare data to timestamped CSV files for pipeline ingestion.

File Naming Convention (MANDATORY):
{entity_type}_{yymmdd}_{hhmm}.csv

Requirements from PRP:
- Generate timestamped CSV files for EHR/ADT/Lab systems
- Place files in appropriate Databricks Volumes for pipeline ingestion  
- Implement data quality validation before CSV generation
- Automatic cleanup of old CSV files to prevent storage bloat
- Ensure CSV format compatibility with Auto Loader ingestion
"""

import csv
import datetime
import os
from typing import List, Dict, Any, Optional
from pathlib import Path
import logging


class HealthcareCSVWriter:
    """
    Write healthcare data to timestamped CSV files with proper formatting for DLT ingestion.
    
    Critical Requirements:
    - File naming: {entity_type}_{yymmdd}_{hhmm}.csv
    - CSV format compatible with Databricks Auto Loader
    - Data quality validation before writing
    - Automatic file cleanup and storage management
    """
    
    def __init__(self, volumes_path: str, cleanup_days: int = 30):
        """
        Initialize CSV writer for healthcare data.
        
        Args:
            volumes_path: Path to Databricks Volumes for CSV ingestion
            cleanup_days: Number of days to retain old CSV files
        """
        self.volumes_path = volumes_path
        self.cleanup_days = cleanup_days
        self.logger = self._setup_logging()
        
        # Ensure volumes path exists
        self._ensure_directory_structure()
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for CSV file operations."""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _ensure_directory_structure(self):
        """Create directory structure for healthcare data storage."""
        try:
            # Create main volumes directory structure
            directories = [
                f"{self.volumes_path}",
                f"{self.volumes_path}/patients",    # EHR_SYSTEM
                f"{self.volumes_path}/claims",      # CLAIMS_SYSTEM  
                f"{self.volumes_path}/medical_events" # ADT_SYSTEM/LAB_SYSTEM
            ]
            
            for directory in directories:
                Path(directory).mkdir(parents=True, exist_ok=True)
                self.logger.info(f"Ensured directory exists: {directory}")
                
        except Exception as e:
            self.logger.error(f"Failed to create directory structure: {e}")
            raise
    
    def generate_timestamped_filename(self, entity_type: str) -> str:
        """
        Generate timestamped filename with required naming convention.
        
        Format: {entity_type}_{yymmdd}_{hhmm}.csv
        Example: patient_250825_0930.csv
        
        Args:
            entity_type: Type of healthcare entity (patients, claims, medical_events)
            
        Returns:
            Timestamped filename string
        """
        now = datetime.datetime.now()
        yymmdd = now.strftime("%y%m%d")
        hhmm = now.strftime("%H%M")
        
        filename = f"{entity_type}_{yymmdd}_{hhmm}.csv"
        self.logger.info(f"Generated filename: {filename}")
        
        return filename
    
    def validate_data_quality(self, data: List[Dict[str, Any]], entity_type: str) -> bool:
        """
        Validate data quality before CSV generation.
        
        Args:
            data: List of healthcare records to validate
            entity_type: Type of entity being validated
            
        Returns:
            True if data passes validation, False otherwise
        """
        if not data:
            self.logger.error(f"No data provided for {entity_type}")
            return False
        
        self.logger.info(f"Validating {len(data)} {entity_type} records...")
        
        # Common validation rules
        errors = []
        
        for i, record in enumerate(data):
            # Check for required fields based on entity type
            if entity_type == "patients":
                required_fields = ['patient_id', 'first_name', 'last_name', 'date_of_birth']
                if not record.get('patient_id', '').startswith('PAT_'):
                    errors.append(f"Row {i}: Invalid patient_id format")
                    
            elif entity_type == "claims":
                required_fields = ['claim_id', 'patient_id', 'claim_amount']
                if not record.get('claim_id', '').startswith('CLM_'):
                    errors.append(f"Row {i}: Invalid claim_id format")
                if not record.get('patient_id', '').startswith('PAT_'):
                    errors.append(f"Row {i}: Invalid patient_id reference")
                    
            elif entity_type == "medical_events":
                required_fields = ['event_id', 'patient_id', 'event_date']
                if not record.get('event_id', '').startswith('EVT_'):
                    errors.append(f"Row {i}: Invalid event_id format")
                if not record.get('patient_id', '').startswith('PAT_'):
                    errors.append(f"Row {i}: Invalid patient_id reference")
            
            # Check required fields are present
            for field in required_fields:
                if not record.get(field):
                    errors.append(f"Row {i}: Missing required field '{field}'")
        
        # Report validation results
        if errors:
            self.logger.error(f"Data validation failed for {entity_type}:")
            for error in errors[:10]:  # Show first 10 errors
                self.logger.error(f"  {error}")
            if len(errors) > 10:
                self.logger.error(f"  ... and {len(errors) - 10} more errors")
            return False
        
        self.logger.info(f"✅ Data validation passed for {entity_type}: {len(data)} records")
        return True
    
    def write_csv_file(self, data: List[Dict[str, Any]], entity_type: str, 
                      source_system: str = None) -> str:
        """
        Write healthcare data to timestamped CSV file.
        
        Args:
            data: List of healthcare records to write
            entity_type: Type of entity (patients, claims, medical_events)
            source_system: Source system identifier (EHR_SYSTEM, ADT_SYSTEM, etc.)
            
        Returns:
            Full path to created CSV file
        """
        # Validate data quality first
        if not self.validate_data_quality(data, entity_type):
            raise ValueError(f"Data validation failed for {entity_type}")
        
        # Generate filename and path
        filename = self.generate_timestamped_filename(entity_type)
        file_path = f"{self.volumes_path}/{entity_type}/{filename}"
        
        try:
            # Write CSV file
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                if data:
                    fieldnames = list(data[0].keys())
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    
                    # Write header
                    writer.writeheader()
                    
                    # Write data rows
                    for record in data:
                        writer.writerow(record)
            
            # Verify file was created successfully
            file_size = os.path.getsize(file_path)
            self.logger.info(f"✅ Successfully wrote {entity_type} CSV: {file_path}")
            self.logger.info(f"   Records: {len(data)}, Size: {file_size:,} bytes")
            
            # Add source system metadata if provided
            if source_system:
                self.logger.info(f"   Source System: {source_system}")
            
            return file_path
            
        except Exception as e:
            self.logger.error(f"Failed to write CSV file {file_path}: {e}")
            raise
    
    def cleanup_old_files(self):
        """
        Clean up old CSV files to prevent storage bloat.
        
        Removes files older than cleanup_days from all entity directories.
        """
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=self.cleanup_days)
        
        entity_dirs = ["patients", "claims", "medical_events"]
        total_cleaned = 0
        
        for entity_dir in entity_dirs:
            dir_path = f"{self.volumes_path}/{entity_dir}"
            if not os.path.exists(dir_path):
                continue
                
            for filename in os.listdir(dir_path):
                if not filename.endswith('.csv'):
                    continue
                    
                file_path = os.path.join(dir_path, filename)
                
                try:
                    # Check file modification time
                    file_mtime = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
                    
                    if file_mtime < cutoff_date:
                        os.remove(file_path)
                        total_cleaned += 1
                        self.logger.info(f"Cleaned up old file: {filename}")
                        
                except Exception as e:
                    self.logger.warning(f"Failed to cleanup file {filename}: {e}")
        
        self.logger.info(f"🧹 Cleanup completed: {total_cleaned} old files removed")
    
    def write_healthcare_dataset(self, healthcare_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, str]:
        """
        Write complete healthcare dataset with all entity types.
        
        Args:
            healthcare_data: Dictionary with 'patients', 'claims', 'medical_events' keys
            
        Returns:
            Dictionary mapping entity types to their CSV file paths
        """
        self.logger.info("🏥 Starting healthcare dataset CSV generation...")
        
        file_paths = {}
        
        # Source system mapping
        source_systems = {
            'patients': 'EHR_SYSTEM',
            'claims': 'CLAIMS_SYSTEM', 
            'medical_events': 'ADT_SYSTEM'  # Could also be LAB_SYSTEM
        }
        
        # Write each entity type
        for entity_type in ['patients', 'claims', 'medical_events']:
            if entity_type in healthcare_data:
                data = healthcare_data[entity_type]
                source_system = source_systems[entity_type]
                
                self.logger.info(f"📝 Writing {entity_type} data ({len(data)} records)...")
                
                file_path = self.write_csv_file(
                    data=data, 
                    entity_type=entity_type,
                    source_system=source_system
                )
                
                file_paths[entity_type] = file_path
        
        # Cleanup old files
        self.cleanup_old_files()
        
        # Summary
        total_records = sum(len(healthcare_data.get(k, [])) for k in file_paths.keys())
        self.logger.info(f"🎯 Healthcare CSV generation completed:")
        self.logger.info(f"   Total files: {len(file_paths)}")
        self.logger.info(f"   Total records: {total_records:,}")
        for entity_type, file_path in file_paths.items():
            record_count = len(healthcare_data.get(entity_type, []))
            self.logger.info(f"   {entity_type}: {record_count:,} records -> {file_path}")
        
        return file_paths


def main():
    """Test the CSV writer functionality."""
    # Example usage for testing
    from synthetic_patient_generator import main as generate_data
    
    # Generate synthetic healthcare data
    print("Generating synthetic healthcare data...")
    healthcare_data = generate_data()
    
    # Write to CSV files
    volumes_path = "/tmp/healthcare_volumes"  # Test path
    csv_writer = HealthcareCSVWriter(volumes_path=volumes_path, cleanup_days=7)
    
    file_paths = csv_writer.write_healthcare_dataset(healthcare_data)
    
    print("\n📁 Generated CSV files:")
    for entity_type, file_path in file_paths.items():
        print(f"   {entity_type}: {file_path}")


if __name__ == "__main__":
    main()