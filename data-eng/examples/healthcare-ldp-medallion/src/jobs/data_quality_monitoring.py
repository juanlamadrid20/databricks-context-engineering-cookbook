# Databricks notebook source
# MAGIC %md
# MAGIC # Healthcare Data Quality Monitoring
# MAGIC 
# MAGIC Comprehensive data quality monitoring across bronze, silver, and gold layers.
# MAGIC Implements real-time quality metrics and SLA compliance reporting.

# COMMAND ----------

# Get job parameters
catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "juan_dev"
schema = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "healthcare_data" 
environment = dbutils.widgets.get("environment") if dbutils.widgets.get("environment") else "dev"

print(f"📊 Data Quality Monitoring Configuration:")
print(f"   Catalog: {catalog}")
print(f"   Schema: {schema}")
print(f"   Environment: {environment}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Metrics Collection

# COMMAND ----------

from pyspark.sql.functions import count, sum as spark_sum, avg, when, lit, current_timestamp, col

# Bronze layer quality metrics
print("🧠 Collecting Bronze Layer Quality Metrics...")

try:
    bronze_patients_metrics = (
        spark.table(f"{catalog}.{schema}.bronze_patients")
        .agg(
            count("*").alias("total_records"),
            spark_sum(when(col("patient_id").isNull(), 1).otherwise(0)).alias("null_patient_ids"),
            spark_sum(when(col("_rescued_data").isNotNull(), 1).otherwise(0)).alias("rescued_records"),
            spark_sum(when((col("age") < 18) | (col("age") > 85), 1).otherwise(0)).alias("invalid_ages")
        )
        .withColumn("table_name", lit("bronze_patients"))
        .withColumn("layer", lit("bronze"))
        .withColumn("quality_check_time", current_timestamp())
    )
    
    bronze_claims_metrics = (
        spark.table(f"{catalog}.{schema}.bronze_claims")
        .agg(
            count("*").alias("total_records"),
            spark_sum(when(col("claim_id").isNull(), 1).otherwise(0)).alias("null_claim_ids"),
            spark_sum(when(col("_rescued_data").isNotNull(), 1).otherwise(0)).alias("rescued_records"),
            spark_sum(when(col("claim_amount") <= 0, 1).otherwise(0)).alias("invalid_amounts")
        )
        .withColumn("table_name", lit("bronze_claims"))
        .withColumn("layer", lit("bronze"))
        .withColumn("quality_check_time", current_timestamp())
    )
    
    print("✅ Bronze layer metrics collected successfully")
except Exception as e:
    print(f"⚠️ Warning: Could not collect bronze metrics: {e}")
    bronze_patients_metrics = None
    bronze_claims_metrics = None

# COMMAND ----------

# Silver layer quality metrics
print("🥈 Collecting Silver Layer Quality Metrics...")

try:
    silver_patients_metrics = (
        spark.table(f"{catalog}.{schema}.silver_patients")
        .agg(
            count("*").alias("total_records"),
            avg("patient_data_quality_score").alias("avg_quality_score"),
            spark_sum(when(col("patient_data_quality_score") >= 0.995, 1).otherwise(0)).alias("high_quality_records"),
            spark_sum(when(col("hipaa_deidentification_applied"), 1).otherwise(0)).alias("hipaa_compliant_records")
        )
        .withColumn("table_name", lit("silver_patients"))
        .withColumn("layer", lit("silver"))
        .withColumn("quality_check_time", current_timestamp())
    )
    
    print("✅ Silver layer metrics collected successfully")
except Exception as e:
    print(f"⚠️ Warning: Could not collect silver metrics: {e}")
    silver_patients_metrics = None

# COMMAND ----------

# Gold layer quality metrics
print("🏆 Collecting Gold Layer Quality Metrics...")

try:
    gold_patients_metrics = (
        spark.table(f"{catalog}.healthcare_data.dim_patients")
        .agg(
            count("*").alias("total_records"),
            countDistinct("patient_natural_key").alias("unique_patients"),
            spark_sum(when(col("is_current_record"), 1).otherwise(0)).alias("current_records"),
            avg("patient_data_quality_score").alias("avg_quality_score")
        )
        .withColumn("table_name", lit("dim_patients"))
        .withColumn("layer", lit("gold"))
        .withColumn("quality_check_time", current_timestamp())
    )
    
    print("✅ Gold layer metrics collected successfully")
except Exception as e:
    print(f"⚠️ Warning: Could not collect gold metrics: {e}")
    gold_patients_metrics = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality SLA Validation

# COMMAND ----------

print("📊 Validating Data Quality SLAs...")

# SLA thresholds
QUALITY_SLA_THRESHOLD = 0.995  # 99.5% as per PRP requirements
HIPAA_COMPLIANCE_THRESHOLD = 1.0  # 100% HIPAA compliance required

sla_results = []

if silver_patients_metrics:
    silver_quality_score = silver_patients_metrics.collect()[0]["avg_quality_score"]
    hipaa_compliance_rate = silver_patients_metrics.collect()[0]["hipaa_compliant_records"] / silver_patients_metrics.collect()[0]["total_records"]
    
    print(f"   Silver Patient Data Quality: {silver_quality_score:.3f} (Target: {QUALITY_SLA_THRESHOLD})")
    print(f"   HIPAA Compliance Rate: {hipaa_compliance_rate:.3f} (Target: {HIPAA_COMPLIANCE_THRESHOLD})")
    
    sla_results.append({
        "metric": "silver_quality_score",
        "actual": silver_quality_score,
        "target": QUALITY_SLA_THRESHOLD,
        "status": "PASS" if silver_quality_score >= QUALITY_SLA_THRESHOLD else "FAIL"
    })
    
    sla_results.append({
        "metric": "hipaa_compliance",
        "actual": hipaa_compliance_rate,
        "target": HIPAA_COMPLIANCE_THRESHOLD,
        "status": "PASS" if hipaa_compliance_rate >= HIPAA_COMPLIANCE_THRESHOLD else "FAIL"
    })

print(f"\n📈 SLA Results: {len([r for r in sla_results if r['status'] == 'PASS'])}/{len(sla_results)} passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Report Generation

# COMMAND ----------

print("📝 Generating Data Quality Report...")

report_timestamp = current_timestamp()

quality_report = {
    "report_timestamp": report_timestamp,
    "environment": environment,
    "catalog": catalog,
    "schema": schema,
    "sla_results": sla_results,
    "overall_status": "PASS" if all(r["status"] == "PASS" for r in sla_results) else "FAIL"
}

print("\n" + "="*60)
print("        HEALTHCARE DATA QUALITY REPORT")
print("="*60)
print(f"Environment: {environment}")
print(f"Timestamp: {report_timestamp}")
print(f"Overall Status: {quality_report['overall_status']}")
print("\nSLA Results:")
for result in sla_results:
    status_icon = "✅" if result["status"] == "PASS" else "❌"
    print(f"  {status_icon} {result['metric']}: {result['actual']:.3f} (target: {result['target']})")

print("="*60)
print("🎉 Data Quality Monitoring Complete!")

# COMMAND ----------

