# Databricks notebook source
"""
Enhanced Healthcare Synthetic Data Generation
Generates realistic healthcare insurance patient data with proper referential integrity.
Produces EXACTLY 3 CSV files: patients.csv, claims.csv, medical_events.csv
"""
# COMMAND ----------
# MAGIC %pip install faker


# COMMAND ----------

import csv
import os
import argparse
import random
from faker import Faker
import numpy as np
from datetime import datetime, timedelta, date
from typing import List, Dict, Tuple

class HealthcareSyntheticDataGenerator:
    def __init__(self, seed: int = 42):
        """Initialize the healthcare data generator with reproducible seed"""
        self.fake = Faker()
        Faker.seed(seed)
        np.random.seed(seed)
        random.seed(seed)
        
        # Clinical reference data
        self.icd10_codes = [
            "Z00.00", "K21.9", "M79.1", "R06.02", "I10", "E11.9", "M25.511", 
            "F32.9", "J06.9", "R50.9", "N39.0", "K59.00", "M54.5", "R11.10",
            "G44.1", "R05", "J02.9", "H66.90", "L03.90", "R51"
        ]
        
        self.cpt_codes = [
            "99213", "99214", "99212", "99215", "99211", "36415", "85025",
            "80053", "93000", "71020", "73610", "99283", "99282", "99284",
            "29881", "66984", "43239", "45380", "52005", "55700"
        ]
        
        self.provider_specialties = [
            "Family Medicine", "Internal Medicine", "Cardiology", "Orthopedics",
            "Emergency Medicine", "Radiology", "Pathology", "Anesthesiology",
            "Surgery", "Pediatrics", "Obstetrics", "Dermatology", "Psychiatry"
        ]
        
        self.insurance_plans = [
            "Blue Cross Blue Shield Premium", "Aetna Health Plan", "Cigna Complete",
            "UnitedHealth Choice", "Humana Gold", "Kaiser Permanente", 
            "Medicare Advantage", "Medicaid Managed Care"
        ]

    def generate_patients(self, n_patients: int) -> List[Dict]:
        """Generate synthetic patient demographics with HIPAA fields"""
        patients = []
        
        for i in range(n_patients):
            # Demographics with realistic distributions
            age = int(np.random.normal(45, 15))
            age = max(18, min(85, age))
            
            sex = np.random.choice(['MALE', 'FEMALE'])
            region = np.random.choice(['NORTHEAST', 'NORTHWEST', 'SOUTHEAST', 'SOUTHWEST'])
            
            # Health metrics
            bmi_base = np.random.normal(28, 6)
            bmi = float(max(16, min(50, bmi_base)))
            
            smoking_prob = 0.15 + (age - 30) * 0.001
            smoker = np.random.random() < smoking_prob
            children = int(max(0, np.random.poisson(1.2)))
            
            # Calculate insurance charges based on risk factors
            base_cost = 3000
            age_factor = (age / 40) ** 1.5
            bmi_factor = 1 + max(0, (bmi - 25) / 25)
            smoking_factor = 2.5 if smoker else 1.0
            children_factor = 1 + (children * 0.1)
            noise = float(np.random.lognormal(0, 0.3))
            
            charges = base_cost * age_factor * bmi_factor * smoking_factor * children_factor * noise
            
            # Generate synthetic PII (will be hashed in silver layer)
            patient_id = f"PAT{1000000 + i:07d}"
            ssn = f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}"
            
            # Calculate date of birth from age
            birth_year = datetime.now().year - age
            date_of_birth = self.fake.date_between(
                start_date=date(birth_year, 1, 1),
                end_date=date(birth_year, 12, 31)
            ).strftime("%Y-%m-%dT00:00:00")
            
            zip_code = self.fake.postcode()
            
            patient = {
                'patient_id': patient_id,
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'age': age,
                'sex': sex,
                'region': region,
                'bmi': round(bmi, 1),
                'smoker': smoker,
                'children': children,
                'charges': round(charges, 2),
                'insurance_plan': np.random.choice(self.insurance_plans),
                'coverage_start_date': self.fake.date_between(
                    start_date=date.today() - timedelta(days=2*365),
                    end_date=date.today()
                ).strftime("%Y-%m-%dT00:00:00"),
                'ssn': ssn,
                'date_of_birth': date_of_birth,
                'zip_code': zip_code,
                'timestamp': datetime.now().isoformat()
            }
            
            patients.append(patient)
            
        return patients

    def generate_claims(self, patients: List[Dict], claims_per_patient_avg: int = 3) -> List[Dict]:
        """Generate realistic insurance claims with proper referential integrity"""
        claims = []
        claim_counter = 1
        
        for patient in patients:
            # Each patient has 2-5 claims on average
            num_claims = max(1, int(np.random.poisson(claims_per_patient_avg)))
            
            for _ in range(num_claims):
                claim_date = self.fake.date_between(
                    start_date=date.today() - timedelta(days=365),
                    end_date=date.today()
                )
                
                # Realistic claim amounts based on procedure type
                base_amount = np.random.choice([150, 300, 500, 1200, 2500, 5000, 15000])
                claim_amount = base_amount * np.random.uniform(0.8, 1.5)
                
                # Processing dates
                approval_date = claim_date + timedelta(days=random.randint(1, 30))
                payment_date = approval_date + timedelta(days=random.randint(1, 15))
                
                claim_status = np.random.choice(
                    ['APPROVED', 'PAID', 'SUBMITTED', 'DENIED'], 
                    p=[0.4, 0.35, 0.15, 0.1]
                )
                
                claim_type = np.random.choice([
                    'OUTPATIENT', 'INPATIENT', 'PHARMACY', 'DENTAL', 'VISION'
                ], p=[0.5, 0.2, 0.15, 0.1, 0.05])
                
                claim = {
                    'claim_id': f"CLM{claim_counter:08d}",
                    'patient_id': patient['patient_id'],
                    'claim_amount': round(claim_amount, 2),
                    'claim_date': claim_date.strftime("%Y-%m-%dT00:00:00"),
                    'approval_date': approval_date.strftime("%Y-%m-%dT00:00:00") if claim_status != 'DENIED' else '',
                    'payment_date': payment_date.strftime("%Y-%m-%dT00:00:00") if claim_status == 'PAID' else '',
                    'diagnosis_code': np.random.choice(self.icd10_codes),
                    'procedure_code': np.random.choice(self.cpt_codes),
                    'diagnosis_description': f"Diagnosis for {np.random.choice(self.icd10_codes)}",
                    'procedure_description': f"Procedure for {np.random.choice(self.cpt_codes)}",
                    'claim_status': claim_status,
                    'claim_type': claim_type,
                    'provider_id': f"PROV{random.randint(10000, 99999)}",
                    'provider_name': f"Dr. {self.fake.last_name()} {np.random.choice(self.provider_specialties)}",
                    'denial_reason': 'Prior authorization required' if claim_status == 'DENIED' else ''
                }
                
                claims.append(claim)
                claim_counter += 1
                
        return claims

    def generate_medical_events(self, patients: List[Dict], events_per_patient_avg: int = 5) -> List[Dict]:
        """Generate medical history events with clinical validity"""
        events = []
        event_counter = 1
        
        for patient in patients:
            # Each patient has 3-8 medical events on average
            num_events = max(1, int(np.random.poisson(events_per_patient_avg)))
            
            for _ in range(num_events):
                event_date = self.fake.date_between(
                    start_date=date.today() - timedelta(days=2*365),
                    end_date=date.today()
                )
                
                event_type = np.random.choice([
                    'OFFICE_VISIT', 'LAB_TEST', 'IMAGING', 'PROCEDURE', 
                    'EMERGENCY', 'SURGERY', 'CONSULTATION'
                ], p=[0.4, 0.2, 0.15, 0.1, 0.08, 0.04, 0.03])
                
                facility_type = np.random.choice([
                    'CLINIC', 'HOSPITAL', 'LABORATORY', 'IMAGING_CENTER', 
                    'URGENT_CARE', 'PHARMACY'
                ], p=[0.4, 0.25, 0.15, 0.1, 0.08, 0.02])
                
                # Generate realistic visit duration based on event type
                if event_type == 'OFFICE_VISIT':
                    duration = random.randint(15, 60)
                elif event_type == 'SURGERY':
                    duration = random.randint(60, 480)
                elif event_type == 'EMERGENCY':
                    duration = random.randint(30, 240)
                else:
                    duration = random.randint(10, 120)
                
                event = {
                    'event_id': f"EVT{event_counter:08d}",
                    'patient_id': patient['patient_id'],
                    'event_date': event_date.strftime("%Y-%m-%dT00:00:00"),
                    'event_type': event_type,
                    'event_description': f"{event_type.replace('_', ' ').title()} for patient care",
                    'medical_provider': f"Dr. {self.fake.last_name()}",
                    'provider_type': np.random.choice(self.provider_specialties),
                    'facility_name': f"{self.fake.city()} {facility_type.replace('_', ' ').title()}",
                    'facility_type': facility_type,
                    'primary_diagnosis': np.random.choice(self.icd10_codes),
                    'secondary_diagnosis': np.random.choice(self.icd10_codes) if random.random() > 0.7 else '',
                    'vital_signs': f'{{"bp": "{random.randint(110,140)}/{random.randint(70,90)}", "pulse": {random.randint(60,100)}, "temp": {round(random.uniform(98.0,101.0),1)}}}',
                    'medications_prescribed': f'["{self.fake.word()}", "{self.fake.word()}"]' if random.random() > 0.5 else '',
                    'visit_duration_minutes': duration,
                    'follow_up_required': random.random() > 0.7,
                    'emergency_flag': event_type == 'EMERGENCY',
                    'admission_flag': event_type in ['SURGERY', 'EMERGENCY'] and random.random() > 0.8
                }
                
                events.append(event)
                event_counter += 1
                
        return events

    def save_to_csv(self, data: List[Dict], filename: str, output_dir: str):
        """Save data to CSV file with timestamped filename"""
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        timestamped_filename = f"{filename.replace('.csv', '')}_{timestamp}.csv"
        full_path = os.path.join(output_dir, timestamped_filename)
        
        if data:
            with open(full_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            
            print(f"✅ Generated {len(data)} records in: {full_path}")
        
        return full_path

    def generate_complete_dataset(self, 
                                n_patients: int = 1000,
                                claims_per_patient: int = 3,
                                events_per_patient: int = 5,
                                output_dir: str = "/tmp/healthcare_data") -> Dict[str, str]:
        """Generate complete healthcare dataset with all 3 entities"""
        
        print(f"🏥 Generating healthcare dataset with {n_patients} patients...")
        
        # Generate patients first (primary entity)
        patients = self.generate_patients(n_patients)
        patients_file = self.save_to_csv(patients, "patients.csv", output_dir)
        
        # Generate claims (referencing patients)
        print(f"💰 Generating claims ({claims_per_patient} avg per patient)...")
        claims = self.generate_claims(patients, claims_per_patient)
        claims_file = self.save_to_csv(claims, "claims.csv", output_dir)
        
        # Generate medical events (referencing patients)
        print(f"🔬 Generating medical events ({events_per_patient} avg per patient)...")
        events = self.generate_medical_events(patients, events_per_patient)
        events_file = self.save_to_csv(events, "medical_events.csv", output_dir)
        
        # Validation summary
        print(f"\n📊 Dataset Summary:")
        print(f"   Patients: {len(patients)}")
        print(f"   Claims: {len(claims)} ({len(claims)/len(patients):.1f} per patient)")
        print(f"   Events: {len(events)} ({len(events)/len(patients):.1f} per patient)")
        print(f"   Referential Integrity: ✅ All claims and events reference valid patients")
        
        return {
            'patients': patients_file,
            'claims': claims_file,
            'events': events_file
        }

# COMMAND ----------

# Get notebook parameters
widget_names = dbutils.widgets.getAll()

catalog = dbutils.widgets.get("catalog") if "catalog" in widget_names else "juan_dev"
schema = dbutils.widgets.get("schema") if "schema" in widget_names else "data_eng"
volumes_path = dbutils.widgets.get("volumes_path") if "volumes_path" in widget_names else "/tmp/healthcare_data"
environment = dbutils.widgets.get("environment") if "environment" in widget_names else "dev"
patient_count = int(dbutils.widgets.get("patient_count")) if "patient_count" in widget_names else 1000
claims_per_patient = int(dbutils.widgets.get("claims_per_patient")) if "claims_per_patient" in widget_names else 3
events_per_patient = int(dbutils.widgets.get("events_per_patient")) if "events_per_patient" in widget_names else 5

print(f"🏥 Generating healthcare dataset for {environment} environment")
print(f"   Catalog: {catalog}")
print(f"   Schema: {schema}")
print(f"   Output path: {volumes_path}")
print(f"   Patients: {patient_count}")



# COMMAND ----------

# Generate the dataset
generator = HealthcareSyntheticDataGenerator()

try:
    files = generator.generate_complete_dataset(
        n_patients=patient_count,
        claims_per_patient=claims_per_patient,
        events_per_patient=events_per_patient,
        output_dir=volumes_path
    )
    
    print(f"\n🎉 Healthcare dataset generation completed!")
    print(f"Files created in: {volumes_path}")
    for entity, filepath in files.items():
        print(f"  {entity}: {os.path.basename(filepath)}")
        
except Exception as e:
    print(f"❌ Error generating dataset: {e}")
    raise