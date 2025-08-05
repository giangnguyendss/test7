# PySpark script: APPLIED QTY (apl_qty) calculation for Life Sciences digital supply chain inventory transactions
# Purpose: Implements the business logic to calculate APPLIED QTY (apl_qty) for inventory transactions, including validation and error handling
# Author: Giang Nguyen
# Date: 2025-08-05
# Description: This script defines the schema, applies business rules for APPLIED QTY calculation, validates input data, handles errors,
#              and produces a DataFrame with the calculated apl_qty field. It supports all required transaction types and edge cases.

# ---------------------------
# Imports and Setup
# ---------------------------

from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import DataFrame  
from pyspark.sql import functions as F  
from pyspark.sql.types import (  
    StructType, StructField, IntegerType, StringType, DateType
)
from typing import Optional  

# ---------------------------
# Schema Definition
# ---------------------------

# Block comment: Define the schema for inventory transaction input data
inventory_schema = StructType([
    StructField("ref_txn_qty", IntegerType(), True),  # Reference transaction quantity
    StructField("cumulative_txn_qty", IntegerType(), True),  # Cumulative transaction quantity
    StructField("cumulative_ref_ord_sched_qty", IntegerType(), True),  # Cumulative reference order scheduled quantity
    StructField("txn_type", StringType(), True),  # Transaction type: 'SALE', 'RETURN', 'DAMAGE', 'ADJUST'
    StructField("inventory_location", StringType(), True),  # Inventory location identifier
    StructField("txn_date", DateType(), True)  # Transaction date
])

# ---------------------------
# Business Logic Implementation
# ---------------------------

def calculate_apl_qty(
    ref_txn_qty: Optional[int],
    cumulative_txn_qty: Optional[int],
    cumulative_ref_ord_sched_qty: Optional[int],
    txn_type: Optional[str]
) -> Optional[int]:
    """
    Calculates the APPLIED QTY (apl_qty) for an inventory transaction.

    Args:
        ref_txn_qty (Optional[int]): Reference transaction quantity.
        cumulative_txn_qty (Optional[int]): Cumulative transaction quantity.
        cumulative_ref_ord_sched_qty (Optional[int]): Cumulative reference order scheduled quantity.
        txn_type (Optional[str]): Transaction type.

    Returns:
        Optional[int]: Calculated applied quantity (apl_qty), or None if input is invalid.
    """
    # Validation: All fields must be present and correct type
    if ref_txn_qty is None:
        raise ValueError("ref_txn_qty is required and cannot be null")
    if not isinstance(ref_txn_qty, int):
        raise TypeError("ref_txn_qty must be an integer")
    if cumulative_txn_qty is None:
        raise ValueError("cumulative_txn_qty is required and cannot be null")
    if not isinstance(cumulative_txn_qty, int):
        raise TypeError("cumulative_txn_qty must be an integer")
    if cumulative_ref_ord_sched_qty is None:
        raise ValueError("cumulative_ref_ord_sched_qty is required and cannot be null")
    if not isinstance(cumulative_ref_ord_sched_qty, int):
        raise TypeError("cumulative_ref_ord_sched_qty must be an integer")
    if txn_type is None:
        raise ValueError("txn_type is required and cannot be null")
    if not isinstance(txn_type, str):
        raise TypeError("txn_type must be a string")
    valid_types = ["SALE", "RETURN", "DAMAGE", "ADJUST"]
    if txn_type not in valid_types:
        raise ValueError("txn_type must be one of: SALE, RETURN, DAMAGE, ADJUST")
    if ref_txn_qty < 0 and txn_type not in ["RETURN", "DAMAGE"]:
        raise ValueError("Negative ref_txn_qty is only allowed for RETURN or DAMAGE transactions")
    # Business rule: If cumulative_ref_ord_sched_qty >= cumulative_txn_qty, then apl_qty = 0
    if cumulative_ref_ord_sched_qty >= cumulative_txn_qty:
        return 0
    # Otherwise, apl_qty = ref_txn_qty
    return ref_txn_qty

# Block comment: Register the business logic as a Spark UDF for DataFrame operations
from pyspark.sql.functions import udf  

calculate_apl_qty_udf = udf(
    calculate_apl_qty,
    IntegerType()
)

# ---------------------------
# Data Processing Function
# ---------------------------

def process_inventory_transactions(input_df: DataFrame) -> DataFrame:
    """
    Processes inventory transactions and calculates the APPLIED QTY (apl_qty) for each record.

    Args:
        input_df (DataFrame): Input DataFrame with inventory transaction records.

    Returns:
        DataFrame: Output DataFrame with all input columns plus the calculated apl_qty column.
    """
    # Validate input schema matches expected schema
    input_fields = set(input_df.columns)
    required_fields = set(["ref_txn_qty", "cumulative_txn_qty", "cumulative_ref_ord_sched_qty", "txn_type", "inventory_location", "txn_date"])
    missing_fields = required_fields - input_fields
    if missing_fields:
        raise ValueError(f"Input DataFrame is missing required fields: {missing_fields}")

    # Apply the APPLIED QTY calculation logic
    output_df = input_df.withColumn(
        "apl_qty",
        calculate_apl_qty_udf(
            F.col("ref_txn_qty"),
            F.col("cumulative_txn_qty"),
            F.col("cumulative_ref_ord_sched_qty"),
            F.col("txn_type")
        )
    )
    return output_df

# ---------------------------
# Example Usage (for demonstration/testing)
# ---------------------------

# Block comment: Example test data for demonstration; in production, input_df would come from upstream pipeline
example_data = [
    (100, 200, 150, "SALE", "LOC001", "2024-03-21"),
    (-20, 180, 160, "RETURN", "LOC004", "2024-03-24"),
    (25, 75, 50, "ADJUST", "LOC012", "2024-04-01"),
    (40, 100, 120, "SALE", "LOC010", "2024-03-30"),
    (None, 100, 90, "SALE", "LOC016", "2024-04-05"),  # Should raise error
    (50, 100, 90, "INVALID", "LOC999", "2024-04-20")  # Should raise error
]
example_schema = StructType([
    StructField("ref_txn_qty", IntegerType(), True),
    StructField("cumulative_txn_qty", IntegerType(), True),
    StructField("cumulative_ref_ord_sched_qty", IntegerType(), True),
    StructField("txn_type", StringType(), True),
    StructField("inventory_location", StringType(), True),
    StructField("txn_date", StringType(), True)
])
example_df = spark.createDataFrame(example_data, schema=example_schema) \
    .withColumn("txn_date", F.to_date("txn_date"))

# Block comment: Try processing the example data, handle and log errors gracefully
def safe_process_inventory_transactions(input_df: DataFrame) -> DataFrame:
    """
    Safely processes inventory transactions, capturing and logging errors for invalid records.

    Args:
        input_df (DataFrame): Input DataFrame with inventory transaction records.

    Returns:
        DataFrame: Output DataFrame with all input columns plus the calculated apl_qty column and error column.
    """
    def safe_calculate_apl_qty(
        ref_txn_qty, cumulative_txn_qty, cumulative_ref_ord_sched_qty, txn_type
    ):
        try:
            return calculate_apl_qty(ref_txn_qty, cumulative_txn_qty, cumulative_ref_ord_sched_qty, txn_type)
        except Exception as e:
            return None

    def error_message(
        ref_txn_qty, cumulative_txn_qty, cumulative_ref_ord_sched_qty, txn_type
    ):
        try:
            calculate_apl_qty(ref_txn_qty, cumulative_txn_qty, cumulative_ref_ord_sched_qty, txn_type)
            return None
        except Exception as e:
            return str(e)

    from pyspark.sql.functions import pandas_udf  
    import pandas as pd  

    @pandas_udf(IntegerType())
    def safe_apl_qty_udf(ref_txn_qty, cumulative_txn_qty, cumulative_ref_ord_sched_qty, txn_type):
        return pd.Series([
            safe_calculate_apl_qty(r, c, s, t)
            for r, c, s, t in zip(ref_txn_qty, cumulative_txn_qty, cumulative_ref_ord_sched_qty, txn_type)
        ])

    @pandas_udf(StringType())
    def error_message_udf(ref_txn_qty, cumulative_txn_qty, cumulative_ref_ord_sched_qty, txn_type):
        return pd.Series([
            error_message(r, c, s, t)
            for r, c, s, t in zip(ref_txn_qty, cumulative_txn_qty, cumulative_ref_ord_sched_qty, txn_type)
        ])

    result_df = input_df.withColumn(
        "apl_qty",
        safe_apl_qty_udf(
            F.col("ref_txn_qty"),
            F.col("cumulative_txn_qty"),
            F.col("cumulative_ref_ord_sched_qty"),
            F.col("txn_type")
        )
    ).withColumn(
        "error_message",
        error_message_udf(
            F.col("ref_txn_qty"),
            F.col("cumulative_txn_qty"),
            F.col("cumulative_ref_ord_sched_qty"),
            F.col("txn_type")
        )
    )
    return result_df

# Block comment: Run the safe processing function and display results
result_df = safe_process_inventory_transactions(example_df)
result_df.show(truncate=False)

# spark.stop()  # Do not stop SparkSession in Databricks
