"""
Synthetic Healthcare Data Generator

CRITICAL: This is FIRST PRIORITY implementation as specified in PRP.
Generate realistic patient/claims/medical_events data with proper referential integrity.

Requirements:
- Generate CSV files with naming convention {entity_type}_{yymmdd}_{hhmm}.csv
- Maintain referential integrity: All claims/medical_events reference valid patient_ids
- Realistic healthcare patterns: Age distributions, BMI, regional demographics
- Data volume: Configurable patient counts with realistic ratios (2-5 claims, 3-8 events per patient)
"""

import random
import datetime
from typing import List, Dict, Any
import numpy as np
from dataclasses import dataclass
import hashlib


@dataclass
class HealthcareDataConfig:
    """Configuration for synthetic healthcare data generation."""
    patient_count: int = 1000
    claims_per_patient_min: int = 2
    claims_per_patient_max: int = 5
    events_per_patient_min: int = 3
    events_per_patient_max: int = 8
    
    # Healthcare data distributions
    age_mean: float = 45.0
    age_std: float = 15.0
    bmi_mean: float = 28.0
    bmi_std: float = 6.0


class SyntheticPatientGenerator:
    """
    Generate realistic synthetic patient data with healthcare patterns.
    
    Data Generation Requirements from PRP:
    - Age distribution (normal μ=45, σ=15)
    - BMI distribution (normal μ=28, σ=6)  
    - Regional demographics with realistic patterns
    - HIPAA-compliant data (no real PII)
    """
    
    def __init__(self, config: HealthcareDataConfig):
        self.config = config
        self.random = random.Random(42)  # Deterministic for testing
        self.np_random = np.random.RandomState(42)
        
        # Healthcare reference data
        self.first_names = [
            "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
            "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
            "Thomas", "Sarah", "Christopher", "Karen", "Charles", "Nancy", "Daniel", "Lisa",
            "Matthew", "Betty", "Anthony", "Helen", "Mark", "Sandra", "Donald", "Donna"
        ]
        
        self.last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
            "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
            "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
            "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young"
        ]
        
        self.regions = ["NORTHEAST", "NORTHWEST", "SOUTHEAST", "SOUTHWEST"]
        self.states = {
            "NORTHEAST": ["NY", "MA", "CT", "NJ", "PA", "ME", "VT", "NH", "RI"],
            "NORTHWEST": ["WA", "OR", "ID", "MT", "WY", "AK"],
            "SOUTHEAST": ["FL", "GA", "SC", "NC", "VA", "TN", "KY", "AL", "MS", "LA"],
            "SOUTHWEST": ["CA", "TX", "AZ", "NM", "NV", "UT", "CO"]
        }
        
        self.insurance_plans = ["PPO", "HMO", "EPO", "HDHP", "Medicare", "Medicaid"]
        self.providers = [f"PCP_{i:04d}" for i in range(1, 501)]  # 500 providers

    def generate_patient_id(self, index: int) -> str:
        """Generate unique patient ID."""
        return f"PAT_{index:06d}"
    
    def generate_demographics(self) -> Dict[str, Any]:
        """Generate realistic patient demographics."""
        # Age distribution (normal, clipped to realistic range)
        age = max(18, min(95, int(self.np_random.normal(self.config.age_mean, self.config.age_std))))
        
        # Gender distribution (slightly more females in healthcare data)
        gender = self.random.choices(["Male", "Female"], weights=[0.48, 0.52])[0]
        
        # Regional distribution
        region = self.random.choice(self.regions)
        state = self.random.choice(self.states[region])
        
        # BMI distribution (normal, clipped to realistic range)  
        bmi = max(16.0, min(50.0, self.np_random.normal(self.config.bmi_mean, self.config.bmi_std)))
        
        # Smoking correlation with age (higher for older patients)
        smoking_probability = min(0.4, (age - 18) / 200 + 0.1)
        smoker = self.random.random() < smoking_probability
        
        # Children distribution (Poisson with lambda=1.2)
        children = max(0, int(self.np_random.poisson(1.2)))
        
        # Insurance premium calculation based on risk factors
        base_premium = 2000.0
        age_factor = 1 + (age - 30) / 100  # Increases with age
        bmi_factor = 1 + max(0, (bmi - 25) / 50)  # BMI penalty
        smoking_factor = 1.5 if smoker else 1.0
        charges = base_premium * age_factor * bmi_factor * smoking_factor * self.random.uniform(0.8, 1.2)
        
        return {
            'age': age,
            'gender': gender,
            'region': region,
            'state': state,
            'bmi': round(bmi, 1),
            'smoker': smoker,
            'children': children,
            'charges': round(charges, 2)
        }
    
    def generate_contact_info(self, state: str) -> Dict[str, Any]:
        """Generate realistic contact information."""
        # Phone number (area codes by region)
        area_codes = {"NY": "212", "CA": "415", "TX": "214", "FL": "305"}
        area_code = area_codes.get(state, "555")
        phone = f"({area_code}) {self.random.randint(100, 999)}-{self.random.randint(1000, 9999)}"
        
        # Email generation
        first = self.random.choice(self.first_names).lower()
        last = self.random.choice(self.last_names).lower()
        domains = ["gmail.com", "yahoo.com", "hotmail.com", "healthmail.com"]
        email = f"{first}.{last}{self.random.randint(1, 999)}@{self.random.choice(domains)}"
        
        # Address generation
        street_num = self.random.randint(100, 9999)
        street_names = ["Main St", "Oak Ave", "Elm Dr", "Park Blvd", "First St", "Second Ave"]
        address = f"{street_num} {self.random.choice(street_names)}"
        
        # ZIP code (5 digits)
        zip_code = f"{self.random.randint(10000, 99999)}"
        
        return {
            'phone': phone,
            'email': email,
            'address_line1': address,
            'zip_code': zip_code
        }
    
    def generate_synthetic_ssn(self) -> str:
        """Generate synthetic SSN for testing (not real)."""
        return f"{self.random.randint(100, 999)}-{self.random.randint(10, 99)}-{self.random.randint(1000, 9999)}"
    
    def generate_patients(self) -> List[Dict[str, Any]]:
        """Generate complete patient dataset with realistic healthcare patterns."""
        patients = []
        
        for i in range(self.config.patient_count):
            patient_id = self.generate_patient_id(i + 1)
            
            # Basic demographics
            demographics = self.generate_demographics()
            
            # Contact information
            contact = self.generate_contact_info(demographics['state'])
            
            # Healthcare identifiers
            mrn = f"MRN_{self.random.randint(100000, 999999)}"
            insurance_id = f"INS_{self.random.randint(10000000, 99999999)}"
            pcp = self.random.choice(self.providers)
            
            # Emergency contact
            emergency_contact = {
                'emergency_contact_name': f"{self.random.choice(self.first_names)} {self.random.choice(self.last_names)}",
                'emergency_contact_phone': f"({self.random.randint(200, 999)}) {self.random.randint(100, 999)}-{self.random.randint(1000, 9999)}"
            }
            
            # Combine all patient data
            patient = {
                'patient_id': patient_id,
                'mrn': mrn,
                'first_name': self.random.choice(self.first_names),
                'last_name': self.random.choice(self.last_names),
                'date_of_birth': self.generate_birth_date(demographics['age']),
                'gender': demographics['gender'],
                'address_line1': contact['address_line1'],
                'city': f"{demographics['region'].title()} City",
                'state': demographics['state'],
                'zip_code': contact['zip_code'],
                'phone': contact['phone'],
                'email': contact['email'],
                'ssn': self.generate_synthetic_ssn(),
                'insurance_id': insurance_id,
                'primary_care_provider': pcp,
                'emergency_contact_name': emergency_contact['emergency_contact_name'],
                'emergency_contact_phone': emergency_contact['emergency_contact_phone'],
                'source_system': 'EHR_SYSTEM',
                'effective_date': datetime.datetime.now().isoformat(),
                'created_at': datetime.datetime.now().isoformat(),
                '_ingested_at': datetime.datetime.now().isoformat()
            }
            
            patients.append(patient)
        
        return patients
    
    def generate_birth_date(self, age: int) -> str:
        """Generate birth date based on age."""
        today = datetime.date.today()
        birth_year = today.year - age
        birth_month = self.random.randint(1, 12)
        birth_day = self.random.randint(1, 28)  # Safe day range
        birth_date = datetime.date(birth_year, birth_month, birth_day)
        return birth_date.strftime("%Y-%m-%d")


class SyntheticClaimsGenerator:
    """Generate realistic insurance claims data linked to patients."""
    
    def __init__(self, config: HealthcareDataConfig, patients: List[Dict[str, Any]]):
        self.config = config
        self.patients = patients
        self.random = random.Random(42)
        
        # Healthcare coding systems
        self.icd10_codes = [
            "Z00.00", "I10", "E11.9", "K21.9", "M79.3", "R06.02", "Z12.31", "N18.3",
            "I25.10", "F32.9", "M25.512", "R50.9", "E78.5", "K59.00", "M54.5", "Z51.11"
        ]
        
        self.cpt_codes = [
            "99213", "99214", "80053", "36415", "85025", "80061", "93000", "99395",
            "99385", "45378", "77067", "G0121", "99396", "99386", "76805", "73721"
        ]
        
        self.claim_statuses = ["submitted", "approved", "denied", "paid"]
        self.service_locations = ["Office", "Hospital", "Emergency", "Laboratory", "Imaging"]

    def generate_claims(self) -> List[Dict[str, Any]]:
        """Generate claims dataset with referential integrity to patients."""
        claims = []
        claim_counter = 1
        
        for patient in self.patients:
            num_claims = self.random.randint(
                self.config.claims_per_patient_min,
                self.config.claims_per_patient_max
            )
            
            for _ in range(num_claims):
                claim_id = f"CLM_{claim_counter:08d}"
                
                # Claim amount based on service type and complexity
                base_amount = self.random.uniform(50, 500)
                if self.random.random() < 0.1:  # 10% high-cost procedures
                    base_amount = self.random.uniform(1000, 5000)
                
                claim = {
                    'claim_id': claim_id,
                    'patient_id': patient['patient_id'],  # CRITICAL: Referential integrity
                    'claim_amount': str(round(base_amount, 2)),
                    'claim_date': self.generate_recent_date(),
                    'diagnosis_code': self.random.choice(self.icd10_codes),
                    'procedure_code': self.random.choice(self.cpt_codes),
                    'claim_status': self.random.choice(self.claim_statuses),
                    'provider_id': patient['primary_care_provider'],
                    'service_location': self.random.choice(self.service_locations),
                    'source_system': 'CLAIMS_SYSTEM',
                    '_ingested_at': datetime.datetime.now().isoformat()
                }
                
                claims.append(claim)
                claim_counter += 1
        
        return claims
    
    def generate_recent_date(self) -> str:
        """Generate recent claim date within last 2 years."""
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=730)
        random_days = self.random.randint(0, (end_date - start_date).days)
        claim_date = start_date + datetime.timedelta(days=random_days)
        return claim_date.strftime("%Y-%m-%d")


class SyntheticMedicalEventsGenerator:
    """Generate realistic medical events data linked to patients."""
    
    def __init__(self, config: HealthcareDataConfig, patients: List[Dict[str, Any]]):
        self.config = config
        self.patients = patients
        self.random = random.Random(42)
        
        self.event_types = ["admission", "discharge", "lab_result", "vital_signs", "clinical_note"]
        self.providers = [f"DR_{i:04d}" for i in range(1, 201)]  # 200 providers
        
        # Clinical results templates
        self.lab_results = [
            "Glucose: 95 mg/dL (Normal)", "Cholesterol: 180 mg/dL", "Hemoglobin: 14.2 g/dL",
            "White Blood Cell: 7500/uL", "Blood Pressure: 120/80 mmHg", "Heart Rate: 72 bpm"
        ]
        
        self.vital_signs = [
            "BP: 118/76, HR: 68, Temp: 98.6F", "BP: 135/85, HR: 88, Temp: 99.1F",
            "BP: 110/70, HR: 65, Temp: 98.2F", "Weight: 165 lbs, Height: 5'8\""
        ]

    def generate_medical_events(self) -> List[Dict[str, Any]]:
        """Generate medical events dataset with referential integrity to patients."""
        events = []
        event_counter = 1
        
        for patient in self.patients:
            num_events = self.random.randint(
                self.config.events_per_patient_min,
                self.config.events_per_patient_max
            )
            
            for _ in range(num_events):
                event_id = f"EVT_{event_counter:08d}"
                event_type = self.random.choice(self.event_types)
                
                # Generate clinical results based on event type
                if event_type == "lab_result":
                    clinical_results = self.random.choice(self.lab_results)
                    source_system = "LAB_SYSTEM"
                elif event_type == "vital_signs":
                    clinical_results = self.random.choice(self.vital_signs)
                    source_system = "LAB_SYSTEM"
                else:
                    clinical_results = f"{event_type.title()} completed successfully"
                    source_system = "ADT_SYSTEM"
                
                event = {
                    'event_id': event_id,
                    'patient_id': patient['patient_id'],  # CRITICAL: Referential integrity
                    'event_date': self.generate_recent_date(),
                    'event_type': event_type,
                    'medical_provider': self.random.choice(self.providers),
                    'event_details': f"Patient {event_type} - routine care",
                    'clinical_results': clinical_results,
                    'source_system': source_system,
                    '_ingested_at': datetime.datetime.now().isoformat()
                }
                
                events.append(event)
                event_counter += 1
        
        return events
    
    def generate_recent_date(self) -> str:
        """Generate recent event date within last year."""
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=365)
        random_days = self.random.randint(0, (end_date - start_date).days)
        event_date = start_date + datetime.timedelta(days=random_days)
        return event_date.strftime("%Y-%m-%d")


def main():
    """Main function to generate all synthetic healthcare data."""
    config = HealthcareDataConfig(patient_count=1000)
    
    print("🏥 Generating Synthetic Healthcare Data...")
    print(f"📊 Configuration: {config.patient_count} patients")
    
    # Generate patients (FIRST - required for referential integrity)
    print("👥 Generating patient data...")
    patient_generator = SyntheticPatientGenerator(config)
    patients = patient_generator.generate_patients()
    print(f"✅ Generated {len(patients)} patients")
    
    # Generate claims (linked to patients)
    print("💰 Generating claims data...")
    claims_generator = SyntheticClaimsGenerator(config, patients)
    claims = claims_generator.generate_claims()
    print(f"✅ Generated {len(claims)} claims")
    
    # Generate medical events (linked to patients)  
    print("🩺 Generating medical events data...")
    events_generator = SyntheticMedicalEventsGenerator(config, patients)
    medical_events = events_generator.generate_medical_events()
    print(f"✅ Generated {len(medical_events)} medical events")
    
    # Validation: Check referential integrity
    print("\n🔍 Validating referential integrity...")
    patient_ids = {p['patient_id'] for p in patients}
    claim_patient_ids = {c['patient_id'] for c in claims}
    event_patient_ids = {e['patient_id'] for e in medical_events}
    
    claims_orphans = claim_patient_ids - patient_ids
    events_orphans = event_patient_ids - patient_ids
    
    if claims_orphans or events_orphans:
        print(f"❌ Referential integrity violation!")
        print(f"   Orphaned claims: {len(claims_orphans)}")
        print(f"   Orphaned events: {len(events_orphans)}")
        return False
    else:
        print("✅ Referential integrity validated successfully")
    
    print(f"\n🎯 Summary:")
    print(f"   Patients: {len(patients)}")
    print(f"   Claims: {len(claims)} ({len(claims)/len(patients):.1f} per patient)")
    print(f"   Medical Events: {len(medical_events)} ({len(medical_events)/len(patients):.1f} per patient)")
    
    return {
        'patients': patients,
        'claims': claims, 
        'medical_events': medical_events
    }


if __name__ == "__main__":
    data = main()