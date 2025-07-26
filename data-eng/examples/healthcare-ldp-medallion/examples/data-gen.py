import csv
import os
from faker import Faker
import numpy as np
from datetime import datetime

class HealthcareSyntheticDataGenerator:
    def __init__(self):
        self.fake = Faker()

    def generate_synthetic_patients_csv(self, n_patients=10000, output_dir="/Volumes/juan_dev/ml/data/inputs/insurance/"):
        """Generate HIPAA-compliant synthetic patient data and save to CSV with a timestamped filename"""
        
        np.random.seed(42)

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Generate unique filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"synthetic_patients_{timestamp}.csv"
        full_path = os.path.join(output_dir, file_name)

        with open(full_path, mode='w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['customer_id', 'age', 'sex', 'bmi', 'children', 'smoker', 'region', 'charges', 'timestamp'])

            for i in range(n_patients):
                age = int(np.random.normal(45, 15))
                age = max(18, min(85, age))

                sex = np.random.choice(['MALE', 'FEMALE'])
                region = np.random.choice(['NORTHEAST', 'NORTHWEST', 'SOUTHEAST', 'SOUTHWEST'])
                bmi_base = np.random.normal(28, 6)
                bmi = float(max(16, min(50, bmi_base)))

                smoking_prob = 0.15 + (age - 30) * 0.001
                smoker = np.random.random() < smoking_prob
                children = int(max(0, np.random.poisson(1.2)))

                base_cost = 3000
                age_factor = (age / 40) ** 1.5
                bmi_factor = 1 + max(0, (bmi - 25) / 25)
                smoking_factor = 2.5 if smoker else 1.0
                children_factor = 1 + (children * 0.1)
                noise = float(np.random.lognormal(0, 0.3))

                charges = base_cost * age_factor * bmi_factor * smoking_factor * children_factor * noise
                timestamp = self.fake.date_time_this_year().isoformat()

                writer.writerow([
                    1000000 + i,
                    age,
                    sex,
                    round(bmi, 1),
                    children,
                    smoker,
                    region,
                    round(charges, 2),
                    timestamp
                ])

        print(f"✅ CSV file created at: {full_path}")
        return full_path

# Example usage
generator = HealthcareSyntheticDataGenerator()
generator.generate_synthetic_patients_csv(5000)
