# Databricks notebook source
"""
Synthetic Healthcare Data Generation Notebook

This notebook orchestrates the generation of synthetic healthcare data
for the patient data medallion pipeline.

CRITICAL: This is FIRST PRIORITY implementation as specified in PRP.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Synthetic Healthcare Data Generation
# MAGIC 
# MAGIC Generate realistic patient/claims/medical_events data with proper referential integrity.

# COMMAND ----------

# Import required libraries
import os
import sys
from datetime import datetime

# Add the data generation modules to path
sys.path.append("../")

# Import our synthetic data generators
from synthetic_patient_generator import (
    HealthcareDataConfig, 
    SyntheticPatientGenerator,
    SyntheticClaimsGenerator, 
    SyntheticMedicalEventsGenerator
)
from csv_file_writer import HealthcareCSVWriter

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Get notebook parameters (passed from Asset Bundle job)
catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "juan_dev"
schema = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "ctx_eng" 
volumes_path = dbutils.widgets.get("volumes_path") if dbutils.widgets.get("volumes_path") else "/Volumes/juan_dev/ctx_eng/raw_data"
patient_count = int(dbutils.widgets.get("patient_count")) if dbutils.widgets.get("patient_count") else 1000

print(f"🏥 Synthetic Healthcare Data Generation")
print(f"📊 Configuration:")
print(f"   Catalog: {catalog}")
print(f"   Schema: {schema}")
print(f"   Volumes Path: {volumes_path}")
print(f"   Patient Count: {patient_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Synthetic Healthcare Data

# COMMAND ----------

# Configure data generation
config = HealthcareDataConfig(patient_count=patient_count)

print(f"👥 Generating patient data...")
patient_generator = SyntheticPatientGenerator(config)
patients = patient_generator.generate_patients()
print(f"✅ Generated {len(patients)} patients")

print(f"💰 Generating claims data...")
claims_generator = SyntheticClaimsGenerator(config, patients)
claims = claims_generator.generate_claims()
print(f"✅ Generated {len(claims)} claims")

print(f"🩺 Generating medical events data...")
events_generator = SyntheticMedicalEventsGenerator(config, patients)
medical_events = events_generator.generate_medical_events()
print(f"✅ Generated {len(medical_events)} medical events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Referential Integrity

# COMMAND ----------

# Validation: Check referential integrity
print("🔍 Validating referential integrity...")
patient_ids = {p['patient_id'] for p in patients}
claim_patient_ids = {c['patient_id'] for c in claims}
event_patient_ids = {e['patient_id'] for e in medical_events}

claims_orphans = claim_patient_ids - patient_ids
events_orphans = event_patient_ids - patient_ids

if claims_orphans or events_orphans:
    print(f"❌ Referential integrity violation!")
    print(f"   Orphaned claims: {len(claims_orphans)}")
    print(f"   Orphaned events: {len(events_orphans)}")
    raise Exception("Referential integrity validation failed")
else:
    print("✅ Referential integrity validated successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write CSV Files

# COMMAND ----------

# Write to timestamped CSV files
healthcare_data = {
    'patients': patients,
    'claims': claims,
    'medical_events': medical_events
}

csv_writer = HealthcareCSVWriter(volumes_path=volumes_path, cleanup_days=30)
file_paths = csv_writer.write_healthcare_dataset(healthcare_data)

print("📁 Generated CSV files:")
for entity_type, file_path in file_paths.items():
    record_count = len(healthcare_data[entity_type])
    print(f"   {entity_type}: {record_count:,} records → {file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"🎯 Synthetic Healthcare Data Generation Summary:")
print(f"   Total Patients: {len(patients):,}")
print(f"   Total Claims: {len(claims):,} ({len(claims)/len(patients):.1f} per patient)")
print(f"   Total Medical Events: {len(medical_events):,} ({len(medical_events)/len(patients):.1f} per patient)")
print(f"   CSV Files Generated: {len(file_paths)}")
print(f"   Volumes Path: {volumes_path}")
print(f"   Generation Time: {datetime.now()}")

# Return success indicator
dbutils.notebook.exit("SUCCESS: Synthetic healthcare data generation completed")