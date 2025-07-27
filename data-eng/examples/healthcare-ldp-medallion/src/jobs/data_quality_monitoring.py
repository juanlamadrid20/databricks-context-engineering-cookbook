# Databricks notebook source
"""
Data Quality Monitoring for Healthcare Pipeline
Monitors pipeline health, data quality metrics, and SLA compliance
"""

# COMMAND ----------

import json
from datetime import datetime
from pyspark.sql.functions import *

# Get configuration parameters
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema") 
environment = dbutils.widgets.get("environment")
pipeline_name = dbutils.widgets.get("pipeline_name")
quality_threshold = float(dbutils.widgets.get("quality_threshold"))

print(f"Monitoring pipeline: {pipeline_name}")
print(f"Quality threshold: {quality_threshold}%")

# COMMAND ----------

# Check silver layer data quality scores
silver_patients_quality = spark.table(f"{catalog}.{schema}.silver_patients_quality_summary").collect()[0]
silver_claims_quality = spark.table(f"{catalog}.{schema}.silver_claims_quality_summary").collect()
silver_events_quality = spark.table(f"{catalog}.{schema}.silver_medical_events_quality_summary").collect()

print(f"Silver Patients Quality: {silver_patients_quality['avg_data_quality_score']:.2f}%")
print(f"HIPAA Compliance Rate: {silver_patients_quality['hipaa_compliance_rate']:.2f}%")

# COMMAND ----------

# Generate quality report
quality_report = {
    "timestamp": datetime.now().isoformat(),
    "pipeline": pipeline_name,
    "environment": environment,
    "quality_metrics": {
        "patients_quality": float(silver_patients_quality['avg_data_quality_score']),
        "hipaa_compliance": float(silver_patients_quality['hipaa_compliance_rate']),
        "sla_met": silver_patients_quality['quality_sla_met']
    }
}

print("Quality Monitoring Report:")
print(json.dumps(quality_report, indent=2))

if quality_report["quality_metrics"]["patients_quality"] < quality_threshold:
    raise Exception(f"Data quality below threshold: {quality_report['quality_metrics']['patients_quality']:.2f}% < {quality_threshold}%")

print("✅ All quality checks passed!")