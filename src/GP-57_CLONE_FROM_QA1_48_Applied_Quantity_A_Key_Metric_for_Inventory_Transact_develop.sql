-- Databricks SQL script: Implements business logic for applied quantity calculation and updates the target table
-- Purpose: Calculate and update the apl_qty field in purgo_playground.f_inv_movmnt_apl_qty using business rules
-- Author: Giang Nguyen
-- Date: 2025-08-05
-- Description: This script defines a scalar SQL UDF for the applied quantity calculation, documents the schema using column comments, and updates the apl_qty field in purgo_playground.f_inv_movmnt_apl_qty. 
--              It handles all business rules, error cases, and data validation as specified in requirements and sample data.

-- =========================================================================================================
/* SECTION: TABLE AND COLUMN DOCUMENTATION */
-- =========================================================================================================

-- Add column comments for documentation and data governance
ALTER TABLE purgo_playground.f_inv_movmnt_apl_qty ALTER COLUMN txn_id COMMENT 'Unique transaction identifier. STRING. May contain special/unicode characters.';
ALTER TABLE purgo_playground.f_inv_movmnt_apl_qty ALTER COLUMN ref_txn_qty COMMENT 'Reference transaction quantity. DECIMAL(3,1). Used in applied quantity logic.';
ALTER TABLE purgo_playground.f_inv_movmnt_apl_qty ALTER COLUMN cumulative_txn_qty COMMENT 'Cumulative transaction quantity. DECIMAL(4,1). Used in applied quantity logic.';
ALTER TABLE purgo_playground.f_inv_movmnt_apl_qty ALTER COLUMN cumulative_ref_ord_sched_qty COMMENT 'Cumulative reference order scheduled quantity. DECIMAL(4,1). Used in applied quantity logic.';
ALTER TABLE purgo_playground.f_inv_movmnt_apl_qty ALTER COLUMN ref_ord_sched_qty COMMENT 'Reference order scheduled quantity. DECIMAL(3,1). Used in applied quantity logic.';
ALTER TABLE purgo_playground.f_inv_movmnt_apl_qty ALTER COLUMN prior_cumulative_txn_qty COMMENT 'Prior cumulative transaction quantity. DECIMAL(3,1). Used in applied quantity logic.';
ALTER TABLE purgo_playground.f_inv_movmnt_apl_qty ALTER COLUMN prior_cumulative_ref_ord_sched_qty COMMENT 'Prior cumulative reference order scheduled quantity. DECIMAL(3,1). Used in applied quantity logic.';
ALTER TABLE purgo_playground.f_inv_movmnt_apl_qty ALTER COLUMN apl_qty COMMENT 'Applied quantity. DECIMAL(5,1). Calculated using business rules. May be NULL if no condition is met or input is invalid.';

-- =========================================================================================================
/* SECTION: UDF DEFINITION FOR apl_qty CALCULATION */
-- =========================================================================================================

-- Drop and create the scalar UDF for applied quantity calculation
DROP FUNCTION IF EXISTS purgo_playground.udf_apl_qty;

-- The UDF implements the business logic for apl_qty calculation as per requirements
-- Returns NULL if any input is NULL or invalid
CREATE OR REPLACE FUNCTION purgo_playground.udf_apl_qty
  (ref_txn_qty DECIMAL(3,1),
   cumulative_txn_qty DECIMAL(4,1),
   cumulative_ref_ord_sched_qty DECIMAL(4,1),
   ref_ord_sched_qty DECIMAL(3,1),
   prior_cumulative_txn_qty DECIMAL(3,1),
   prior_cumulative_ref_ord_sched_qty DECIMAL(3,1))
RETURNS DECIMAL(5,1)
RETURN
  CASE
    -- Error/NULL handling: If any input is NULL, return NULL
    WHEN ref_txn_qty IS NULL OR cumulative_txn_qty IS NULL OR cumulative_ref_ord_sched_qty IS NULL
      OR ref_ord_sched_qty IS NULL OR prior_cumulative_txn_qty IS NULL OR prior_cumulative_ref_ord_sched_qty IS NULL
      THEN NULL
    -- Condition 1: ref_txn_qty > 0 and cumulative_txn_qty >= cumulative_ref_ord_sched_qty
    WHEN ref_txn_qty > 0 AND cumulative_txn_qty >= cumulative_ref_ord_sched_qty THEN
      CASE
        WHEN prior_cumulative_ref_ord_sched_qty < prior_cumulative_txn_qty
          THEN ref_ord_sched_qty - (prior_cumulative_txn_qty - prior_cumulative_ref_ord_sched_qty)
        ELSE ref_ord_sched_qty
      END
    -- Condition 2: ref_txn_qty > 0 and cumulative_ref_ord_sched_qty >= cumulative_txn_qty
    WHEN ref_txn_qty > 0 AND cumulative_ref_ord_sched_qty >= cumulative_txn_qty THEN
      CASE
        WHEN prior_cumulative_ref_ord_sched_qty > prior_cumulative_txn_qty
          THEN ref_txn_qty - (prior_cumulative_ref_ord_sched_qty - prior_cumulative_txn_qty)
        ELSE ref_txn_qty
      END
    -- Condition 3: ref_txn_qty < 0, cumulative_txn_qty != 0, cumulative_ref_ord_sched_qty > 0
    WHEN ref_txn_qty < 0 AND cumulative_txn_qty != 0 AND cumulative_ref_ord_sched_qty > 0 THEN
      ref_txn_qty
    -- Default: None of the above
    ELSE NULL
  END;

-- =========================================================================================================
/* SECTION: UPDATE apl_qty FIELD IN TARGET TABLE */
-- =========================================================================================================

-- Update the apl_qty field in the main table using the UDF
-- This statement applies the business logic to all rows in the table
UPDATE purgo_playground.f_inv_movmnt_apl_qty
SET apl_qty = purgo_playground.udf_apl_qty(
  ref_txn_qty,
  cumulative_txn_qty,
  cumulative_ref_ord_sched_qty,
  ref_ord_sched_qty,
  prior_cumulative_txn_qty,
  prior_cumulative_ref_ord_sched_qty
);

-- =========================================================================================================
/* SECTION: VALIDATION QUERY - SHOW UPDATED ROWS */
-- =========================================================================================================

-- CTE: Select all rows with calculated apl_qty for review and validation
WITH updated_apl_qty_cte AS (
  SELECT
    txn_id,
    ref_txn_qty,
    cumulative_txn_qty,
    cumulative_ref_ord_sched_qty,
    ref_ord_sched_qty,
    prior_cumulative_txn_qty,
    prior_cumulative_ref_ord_sched_qty,
    apl_qty
  FROM purgo_playground.f_inv_movmnt_apl_qty
)
SELECT * FROM updated_apl_qty_cte
ORDER BY txn_id;

-- =========================================================================================================
/* END OF SCRIPT */
-- =========================================================================================================
