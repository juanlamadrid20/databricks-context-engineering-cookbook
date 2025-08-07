# Databricks notebook source
# MAGIC %md
# MAGIC # Healthcare Synthetic Data Generation
# MAGIC 
# MAGIC Enhanced synthetic data generation for HIPAA-compliant healthcare patient pipeline.
# MAGIC Generates exactly 3 CSV files with referential integrity: patients.csv, claims.csv, medical_events.csv

# COMMAND ----------
# MAGIC %pip install faker


# COMMAND ----------

import csv
import os
from faker import Faker
import numpy as np
from datetime import datetime, timedelta
import random
from typing import List, Dict, Tuple

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Parameters

# COMMAND ----------

# Get job parameters
catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "juan_dev"
schema = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "healthcare_data" 
volumes_path = dbutils.widgets.get("volumes_path") if dbutils.widgets.get("volumes_path") else "/Volumes/juan_dev/healthcare_data/raw_data"
environment = dbutils.widgets.get("environment") if dbutils.widgets.get("environment") else "dev"

print(f"📊 Healthcare Data Generation Configuration:")
print(f"   Catalog: {catalog}")
print(f"   Schema: {schema}")
print(f"   Volumes Path: {volumes_path}")
print(f"   Environment: {environment}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Healthcare Synthetic Data Generator Class

# COMMAND ----------

class HealthcareSyntheticDataGenerator:
    def __init__(self, seed: int = 42):
        self.fake = Faker()
        np.random.seed(seed)
        random.seed(seed)
        Faker.seed(seed)
        
        # Healthcare domain constants from INITIAL.md
        self.regions = ["NORTHEAST", "NORTHWEST", "SOUTHEAST", "SOUTHWEST"]
        self.sex_values = ["MALE", "FEMALE"]
        self.claim_statuses = ["submitted", "approved", "denied", "paid"]
        self.event_types = ["checkup", "emergency", "surgery", "consultation", "diagnostic", "treatment"]
        
        # Insurance plans
        self.insurance_plans = ["BASIC_PLAN", "PREMIUM_PLAN", "FAMILY_PLAN", "SENIOR_PLAN", "CATASTROPHIC_PLAN"]
        
        # Medical providers
        self.medical_providers = [
            "General Hospital", "City Medical Center", "Regional Clinic", 
            "University Hospital", "Community Health", "Specialty Center",
            "Emergency Care", "Urgent Care Plus", "Family Practice", "Internal Medicine"
        ]
        
        # ICD-10 diagnosis codes (sample)
        self.diagnosis_codes = [
            "Z00.00", "I10", "E11.9", "M79.3", "J06.9", "R06.02", 
            "Z12.31", "F41.1", "M25.561", "K21.9"
        ]
        
        # CPT procedure codes (sample)
        self.procedure_codes = [
            "99213", "99214", "99395", "99396", "36415", "80053",
            "93000", "71020", "80061", "85025"
        ]

    def generate_patients_data(self, n_patients: int = 10000) -> List[Dict]:
        """Generate realistic patient demographics with healthcare-specific distributions."""
        patients = []
        
        print(f"🏥 Generating {n_patients:,} patient records...")
        
        for i in range(n_patients):
            # Demographics with specified distributions
            age = max(18, min(85, int(np.random.normal(45, 15))))
            sex = np.random.choice(self.sex_values)
            region = np.random.choice(self.regions)
            
            # Health metrics
            bmi = max(16, min(50, np.random.normal(28, 6)))
            
            # Age-correlated smoking probability
            smoking_prob = 0.15 + max(0, (age - 30) * 0.001)
            smoker = np.random.random() < smoking_prob
            
            # Number of children (Poisson λ=1.2)
            children = max(0, int(np.random.poisson(1.2)))
            
            # Calculate insurance premium based on risk factors
            base_cost = 3000
            age_factor = (age / 40) ** 1.5
            bmi_factor = 1 + max(0, (bmi - 25) / 25)
            smoking_factor = 2.5 if smoker else 1.0
            children_factor = 1 + (children * 0.1)
            noise = np.random.lognormal(0, 0.3)
            
            charges = base_cost * age_factor * bmi_factor * smoking_factor * children_factor * noise
            
            # Insurance and temporal data
            insurance_plan = np.random.choice(self.insurance_plans)
            coverage_start = self.fake.date_between(start_date="-2y", end_date="today")
            timestamp = self.fake.date_time_this_year()
            
            patient = {
                "patient_id": f"PAT{1000000 + i:07d}",
                "first_name": self.fake.first_name(),
                "last_name": self.fake.last_name(),
                "age": age,
                "sex": sex,
                "region": region,
                "bmi": round(bmi, 1),
                "smoker": smoker,
                "children": children,
                "charges": round(charges, 2),
                "insurance_plan": insurance_plan,
                "coverage_start_date": coverage_start.isoformat(),
                "timestamp": timestamp.isoformat()
            }
            
            patients.append(patient)
            
            if (i + 1) % 1000 == 0:
                print(f"   Generated {i + 1:,} patients...")
        
        print(f"✅ Generated {len(patients):,} patient records")
        return patients

    def generate_claims_data(self, patients: List[Dict], avg_claims_per_patient: float = 2.5) -> List[Dict]:
        """Generate insurance claims with referential integrity to patients."""
        claims = []
        total_claims = int(len(patients) * avg_claims_per_patient)
        
        print(f"💰 Generating ~{total_claims:,} claims records...")
        
        claim_counter = 0
        
        for patient in patients:
            # Each patient has 2-5 claims (Poisson distribution around avg)
            num_claims = max(1, min(8, int(np.random.poisson(avg_claims_per_patient))))
            
            for _ in range(num_claims):
                claim_counter += 1
                
                # Claim financial details
                base_amount = np.random.lognormal(6, 1.5)  # Log-normal distribution for medical costs
                claim_amount = max(50, min(50000, base_amount))
                
                # Claim date within last 2 years
                claim_date = self.fake.date_between(start_date="-2y", end_date="today")
                
                # Higher-cost claims more likely to be denied/reviewed
                if claim_amount > 10000:
                    claim_status = np.random.choice(self.claim_statuses, p=[0.1, 0.6, 0.2, 0.1])
                else:
                    claim_status = np.random.choice(self.claim_statuses, p=[0.05, 0.8, 0.1, 0.05])
                
                # Medical codes
                diagnosis_code = np.random.choice(self.diagnosis_codes)
                procedure_code = np.random.choice(self.procedure_codes)
                
                timestamp = self.fake.date_time_between(start_date=claim_date, end_date="now")
                
                claim = {
                    "claim_id": f"CLM{2000000 + claim_counter:07d}",
                    "patient_id": patient["patient_id"],
                    "claim_amount": round(claim_amount, 2),
                    "claim_date": claim_date.isoformat(),
                    "diagnosis_code": diagnosis_code,
                    "procedure_code": procedure_code,
                    "claim_status": claim_status,
                    "timestamp": timestamp.isoformat()
                }
                
                claims.append(claim)
        
        print(f"✅ Generated {len(claims):,} claims records")
        return claims

    def generate_medical_events_data(self, patients: List[Dict], avg_events_per_patient: float = 5.0) -> List[Dict]:
        """Generate medical events with referential integrity to patients."""
        events = []
        total_events = int(len(patients) * avg_events_per_patient)
        
        print(f"🏥 Generating ~{total_events:,} medical events records...")
        
        event_counter = 0
        
        for patient in patients:
            # Each patient has 3-8 medical events
            num_events = max(2, min(12, int(np.random.poisson(avg_events_per_patient))))
            
            for _ in range(num_events):
                event_counter += 1
                
                # Event date within last 2 years
                event_date = self.fake.date_between(start_date="-2y", end_date="today")
                
                # Event type - checkups more common
                event_type = np.random.choice(
                    self.event_types, 
                    p=[0.4, 0.1, 0.05, 0.2, 0.15, 0.1]  # checkup most common
                )
                
                # Medical provider
                medical_provider = np.random.choice(self.medical_providers)
                
                timestamp = self.fake.date_time_between(start_date=event_date, end_date="now")
                
                event = {
                    "event_id": f"EVT{3000000 + event_counter:07d}",
                    "patient_id": patient["patient_id"],
                    "event_date": event_date.isoformat(),
                    "event_type": event_type,
                    "medical_provider": medical_provider,
                    "timestamp": timestamp.isoformat()
                }
                
                events.append(event)
        
        print(f"✅ Generated {len(events):,} medical events records")
        return events

    def write_csv_to_volumes(self, data: List[Dict], filename: str, volumes_path: str):
        """Write data to CSV in Databricks Volumes with timestamp."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        full_filename = f"{filename}_{timestamp}.csv"
        full_path = f"{volumes_path}/{full_filename}"
        
        print(f"📝 Writing {len(data):,} records to {full_path}")
        
        if data:
            # Import schema definitions
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
            
            # Define schemas explicitly to avoid inference errors
            if filename == "patients":
                schema = StructType([
                    StructField("patient_id", StringType(), True),
                    StructField("first_name", StringType(), True),
                    StructField("last_name", StringType(), True),
                    StructField("age", IntegerType(), True),
                    StructField("sex", StringType(), True),
                    StructField("region", StringType(), True),
                    StructField("bmi", DoubleType(), True),
                    StructField("smoker", BooleanType(), True),
                    StructField("children", IntegerType(), True),
                    StructField("charges", DoubleType(), True),
                    StructField("insurance_plan", StringType(), True),
                    StructField("coverage_start_date", StringType(), True),
                    StructField("timestamp", StringType(), True)
                ])
            elif filename == "claims":
                schema = StructType([
                    StructField("claim_id", StringType(), True),
                    StructField("patient_id", StringType(), True),
                    StructField("claim_amount", DoubleType(), True),
                    StructField("claim_date", StringType(), True),
                    StructField("diagnosis_code", StringType(), True),
                    StructField("procedure_code", StringType(), True),
                    StructField("claim_status", StringType(), True),
                    StructField("timestamp", StringType(), True)
                ])
            elif filename == "medical_events":
                schema = StructType([
                    StructField("event_id", StringType(), True),
                    StructField("patient_id", StringType(), True),
                    StructField("event_date", StringType(), True),
                    StructField("event_type", StringType(), True),
                    StructField("medical_provider", StringType(), True),
                    StructField("timestamp", StringType(), True)
                ])
            else:
                # Fallback to inference for unknown filename types
                schema = None
            
            # Convert data to Spark DataFrame with explicit schema
            if schema:
                df = spark.createDataFrame(data, schema)
            else:
                df = spark.createDataFrame(data)
            
            # Write as single CSV file with header
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{volumes_path}/temp_{filename}")
            
            # Move the part file to the final location
            temp_files = dbutils.fs.ls(f"{volumes_path}/temp_{filename}")
            csv_file = [f for f in temp_files if f.name.startswith('part-') and f.name.endswith('.csv')][0]
            
            dbutils.fs.cp(csv_file.path, full_path)
            dbutils.fs.rm(f"{volumes_path}/temp_{filename}", True)
            
            print(f"✅ Successfully wrote {full_path}")
            return full_path
        else:
            print(f"❌ No data to write for {filename}")
            return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Healthcare Data

# COMMAND ----------

# Initialize generator
generator = HealthcareSyntheticDataGenerator(seed=42)

print("🚀 Starting Healthcare Synthetic Data Generation...")
print("=" * 60)

# Ensure volumes directory exists
try:
    dbutils.fs.ls(volumes_path)
    print(f"✅ Volumes path {volumes_path} exists")
except:
    print(f"❌ Creating volumes path {volumes_path}")
    dbutils.fs.mkdirs(volumes_path)

# Generate data with exact ratios from INITIAL.md
n_patients = 10000
patients_data = generator.generate_patients_data(n_patients)

# Generate claims (2-5 per patient, average 2.5)
claims_data = generator.generate_claims_data(patients_data, avg_claims_per_patient=2.5)

# Generate medical events (3-8 per patient, average 5.0)  
events_data = generator.generate_medical_events_data(patients_data, avg_events_per_patient=5.0)

print("=" * 60)
print("📊 Data Generation Summary:")
print(f"   Patients: {len(patients_data):,}")
print(f"   Claims: {len(claims_data):,} (avg {len(claims_data)/len(patients_data):.1f} per patient)")
print(f"   Medical Events: {len(events_data):,} (avg {len(events_data)/len(patients_data):.1f} per patient)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Volumes

# COMMAND ----------

print("📁 Writing CSV files to Databricks Volumes...")

# Write all three CSV files
patients_file = generator.write_csv_to_volumes(patients_data, "patients", volumes_path)
claims_file = generator.write_csv_to_volumes(claims_data, "claims", volumes_path) 
events_file = generator.write_csv_to_volumes(events_data, "medical_events", volumes_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation and Summary

# COMMAND ----------

print("🔍 Validating Generated Data...")

# Validate referential integrity
patient_ids = set(p["patient_id"] for p in patients_data)
claims_patient_ids = set(c["patient_id"] for c in claims_data)
events_patient_ids = set(e["patient_id"] for e in events_data)

claims_orphans = claims_patient_ids - patient_ids
events_orphans = events_patient_ids - patient_ids

print(f"📋 Referential Integrity Validation:")
print(f"   Claims with valid patient_id: {len(claims_patient_ids - claims_orphans):,}/{len(claims_data):,}")
print(f"   Events with valid patient_id: {len(events_patient_ids - events_orphans):,}/{len(events_data):,}")

if claims_orphans:
    print(f"❌ Found {len(claims_orphans)} orphaned claims!")
else:
    print("✅ All claims have valid patient references")

if events_orphans:
    print(f"❌ Found {len(events_orphans)} orphaned events!")
else:
    print("✅ All events have valid patient references")

# Data quality summary
print(f"\n📈 Data Quality Summary:")
print(f"   Patient age range: {min(p['age'] for p in patients_data)}-{max(p['age'] for p in patients_data)} years")
print(f"   BMI range: {min(p['bmi'] for p in patients_data):.1f}-{max(p['bmi'] for p in patients_data):.1f}")
print(f"   Claims amount range: ${min(c['claim_amount'] for c in claims_data):,.2f}-${max(c['claim_amount'] for c in claims_data):,.2f}")
print(f"   Smokers: {sum(1 for p in patients_data if p['smoker']):,} ({sum(1 for p in patients_data if p['smoker'])/len(patients_data)*100:.1f}%)")

print("\n🎉 Healthcare Synthetic Data Generation Complete!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generated Files
# MAGIC 
# MAGIC The following files have been created in the Databricks Volumes:
# MAGIC 1. **patients.csv** - Patient demographics and insurance information  
# MAGIC 2. **claims.csv** - Insurance claims with medical codes and financial details
# MAGIC 3. **medical_events.csv** - Medical events and provider interactions
# MAGIC 
# MAGIC All files maintain referential integrity with realistic healthcare data distributions per INITIAL.md requirements.

# COMMAND ----------

