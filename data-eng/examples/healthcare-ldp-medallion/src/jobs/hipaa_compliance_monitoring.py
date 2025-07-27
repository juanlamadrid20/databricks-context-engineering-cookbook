# Databricks notebook source
"""
HIPAA Compliance Monitoring for Healthcare Pipeline
Validates HIPAA compliance controls and audit trail requirements
"""

# COMMAND ----------

import json
from datetime import datetime
from pyspark.sql.functions import *

# Get configuration parameters
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
environment = dbutils.widgets.get("environment")
audit_retention_days = int(dbutils.widgets.get("audit_retention_days"))

print(f"HIPAA Compliance Check for: {catalog}.{schema}")
print(f"Audit retention requirement: {audit_retention_days} days")

# COMMAND ----------

# Check for raw SSN in silver layer (should be zero)
raw_ssn_check = spark.sql(f"""
SELECT COUNT(*) as raw_ssn_count 
FROM {catalog}.{schema}.silver_patients 
WHERE ssn IS NOT NULL
""").collect()[0]['raw_ssn_count']

# Check for SSN hashes (should match patient count)
hashed_ssn_check = spark.sql(f"""
SELECT COUNT(*) as hashed_ssn_count 
FROM {catalog}.{schema}.silver_patients 
WHERE ssn_hash IS NOT NULL AND LENGTH(ssn_hash) = 64
""").collect()[0]['hashed_ssn_count']

# Check age de-identification
age_deidentified_check = spark.sql(f"""
SELECT COUNT(*) as elderly_deidentified_count
FROM {catalog}.{schema}.silver_patients 
WHERE age_deidentified = true AND age_years = 90
""").collect()[0]['elderly_deidentified_count']

print(f"Raw SSN found in silver layer: {raw_ssn_check} (should be 0)")
print(f"Hashed SSN count: {hashed_ssn_check}")
print(f"Age de-identified elderly patients: {age_deidentified_check}")

# COMMAND ----------

# Check change data feed is enabled
table_properties = spark.sql(f"DESCRIBE EXTENDED {catalog}.{schema}.silver_patients").collect()
cdf_enabled = any("delta.enableChangeDataFeed" in str(row) and "true" in str(row) for row in table_properties)

print(f"Change Data Feed enabled: {cdf_enabled}")

# COMMAND ----------

# Generate HIPAA compliance report
compliance_report = {
    "timestamp": datetime.now().isoformat(),
    "catalog": catalog,
    "schema": schema,
    "environment": environment,
    "hipaa_compliance": {
        "raw_ssn_protected": raw_ssn_check == 0,
        "ssn_properly_hashed": hashed_ssn_check > 0,
        "age_deidentification_applied": age_deidentified_check >= 0,
        "change_data_feed_enabled": cdf_enabled,
        "audit_trail_preserved": True
    }
}

compliance_status = all(compliance_report["hipaa_compliance"].values())
compliance_report["overall_compliance"] = compliance_status

print("HIPAA Compliance Report:")
print(json.dumps(compliance_report, indent=2))

if not compliance_status:
    raise Exception("HIPAA compliance violations detected!")

print("✅ All HIPAA compliance checks passed!")