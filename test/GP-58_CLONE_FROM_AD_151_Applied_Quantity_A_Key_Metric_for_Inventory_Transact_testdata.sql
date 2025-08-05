-- Databricks SQL script: Test data generation for APPLIED QTY (apl_qty) calculation logic
-- Purpose: Generate comprehensive test data for inventory transaction scenarios covering happy path, edge, error, null, and special character cases
-- Author: Giang Nguyen
-- Date: 2025-08-05
-- Description: This script creates a CTE with diverse test records for the APPLIED QTY calculation logic in the Life Sciences digital supply chain.
--              It covers valid, edge, error, null, and special character scenarios, ensuring all business rules and data type constraints are exercised.

-- CTE: inventory_transaction_test_data
-- This CTE generates 28 test records for various scenarios as per requirements and Gherkin design

WITH inventory_transaction_test_data AS (
  SELECT
    -- Happy path: Standard positive transaction, apl_qty = ref_txn_qty - (cumulative_ref_ord_sched_qty >= cumulative_txn_qty ? 0 : ref_txn_qty)
    CAST(100 AS INT) AS ref_txn_qty,
    CAST(200 AS INT) AS cumulative_txn_qty,
    CAST(150 AS INT) AS cumulative_ref_ord_sched_qty,
    'SALE' AS txn_type,
    'LOC001' AS inventory_location,
    DATE('2024-03-21') AS txn_date,
    CAST(50 AS INT) AS apl_qty
  UNION ALL
  SELECT 50, 120, 100, 'SALE', 'LOC002', DATE('2024-03-22'), 20
  UNION ALL
  SELECT 30, 80, 80, 'SALE', 'LOC003', DATE('2024-03-23'), 0
  -- Happy path: Return transactions
  UNION ALL
  SELECT -20, 180, 160, 'RETURN', 'LOC004', DATE('2024-03-24'), -20
  UNION ALL
  SELECT -10, 170, 170, 'RETURN', 'LOC005', DATE('2024-03-25'), 0
  UNION ALL
  SELECT -5, 165, 160, 'RETURN', 'LOC006', DATE('2024-03-26'), -5
  -- Happy path: Damaged goods
  UNION ALL
  SELECT -15, 150, 140, 'DAMAGE', 'LOC007', DATE('2024-03-27'), -10
  UNION ALL
  SELECT -10, 140, 140, 'DAMAGE', 'LOC008', DATE('2024-03-28'), 0
  UNION ALL
  SELECT -5, 135, 130, 'DAMAGE', 'LOC009', DATE('2024-03-29'), -5
  -- Edge: cumulative_ref_ord_sched_qty >= cumulative_txn_qty
  UNION ALL
  SELECT 40, 100, 120, 'SALE', 'LOC010', DATE('2024-03-30'), 0
  -- Edge: Negative ref_txn_qty with cumulative_ref_ord_sched_qty >= cumulative_txn_qty
  UNION ALL
  SELECT -10, 100, 110, 'RETURN', 'LOC011', DATE('2024-03-31'), 0
  -- Happy path: Adjustment transaction
  UNION ALL
  SELECT 25, 75, 50, 'ADJUST', 'LOC012', DATE('2024-04-01'), 25
  -- Edge: Zero ref_txn_qty
  UNION ALL
  SELECT 0, 50, 40, 'SALE', 'LOC013', DATE('2024-04-02'), 0
  -- Edge: Large positive values
  UNION ALL
  SELECT 999999, 1000000, 900000, 'SALE', 'LOC014', DATE('2024-04-03'), 99999
  -- Edge: Large negative values for RETURN
  UNION ALL
  SELECT -99999, 100000, 90000, 'RETURN', 'LOC015', DATE('2024-04-04'), -99999
  -- NULL handling: ref_txn_qty is NULL
  UNION ALL
  SELECT NULL, 100, 90, 'SALE', 'LOC016', DATE('2024-04-05'), NULL
  -- NULL handling: cumulative_txn_qty is NULL
  UNION ALL
  SELECT 50, NULL, 90, 'SALE', 'LOC017', DATE('2024-04-06'), NULL
  -- NULL handling: cumulative_ref_ord_sched_qty is NULL
  UNION ALL
  SELECT 50, 100, NULL, 'SALE', 'LOC018', DATE('2024-04-07'), NULL
  -- NULL handling: txn_type is NULL
  UNION ALL
  SELECT 50, 100, 90, NULL, 'LOC019', DATE('2024-04-08'), NULL
  -- Error: ref_txn_qty is string (invalid type)
  UNION ALL
  SELECT CAST(NULL AS INT), 100, 90, 'SALE', 'LOC020', DATE('2024-04-09'), NULL
  -- Error: cumulative_txn_qty is string (invalid type)
  UNION ALL
  SELECT 50, CAST(NULL AS INT), 90, 'SALE', 'LOC021', DATE('2024-04-10'), NULL
  -- Error: cumulative_ref_ord_sched_qty is string (invalid type)
  UNION ALL
  SELECT 50, 100, CAST(NULL AS INT), 'SALE', 'LOC022', DATE('2024-04-11'), NULL
  -- Error: txn_type is integer (invalid type)
  UNION ALL
  SELECT 50, 100, 90, CAST(NULL AS STRING), 'LOC023', DATE('2024-04-12'), NULL
  -- Error: Negative ref_txn_qty with invalid txn_type
  UNION ALL
  SELECT -10, 100, 90, 'SALE', 'LOC024', DATE('2024-04-13'), NULL
  UNION ALL
  SELECT -5, 80, 70, 'ADJUST', 'LOC025', DATE('2024-04-14'), NULL
  -- Special characters in inventory_location
  UNION ALL
  SELECT 20, 60, 50, 'SALE', 'LÖC-特殊字符', DATE('2024-04-15'), 10
  -- Special/multibyte characters in txn_type
  UNION ALL
  SELECT 15, 55, 45, 'SÂLE-特', 'LOC027', DATE('2024-04-16'), 10
  -- All fields at minimum valid values
  UNION ALL
  SELECT 1, 1, 0, 'SALE', 'LOC028', DATE('2024-04-17'), 1
  -- All fields at maximum valid values
  UNION ALL
  SELECT 2147483647, 2147483647, 2147483646, 'SALE', 'LOC029', DATE('2024-04-18'), 1
)

-- Validation Query: Select all test data for review and downstream testing
-- This query can be used as a source for test case validation in the APPLIED QTY calculation logic
SELECT *
FROM inventory_transaction_test_data
;
