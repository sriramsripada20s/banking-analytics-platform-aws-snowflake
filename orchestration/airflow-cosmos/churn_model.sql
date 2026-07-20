-- Consolidated churn model setup: train, score, and log under a single role.
-- Co-authored with CoCo
-- ============================================================
-- CHURN MODEL SETUP — FINAL, CONSOLIDATED VERSION
-- Everything runs as FINTECH_ML_ENGINEER, consistently, start to
-- finish. This sidesteps the entire cross-role ownership problem
-- that caused today's grant back-and-forth with FINTECH_TRANSFORMER.
-- ============================================================

-- Step 1: Clean up any model object left over from earlier
-- mixed-role attempts, so it gets recreated fresh under the
-- right owner this time.
USE ROLE ACCOUNTADMIN;
DROP MODEL IF EXISTS FINTECH_PROD.ML.CUSTOMER_CHURN_MODEL;
-- Drop the stale ACCOUNTADMIN-owned predictions table so SCORE_CHURN_MODEL recreates it
-- fresh, fully owned by FINTECH_ML_ENGINEER (single-role ownership).
DROP TABLE IF EXISTS FINTECH_PROD.ML.CHURN_PREDICTIONS;

-- Step 2: Create the training log table
CREATE TABLE IF NOT EXISTS FINTECH_PROD.ML.CHURN_MODEL_TRAINING_LOG (
    trained_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    version_name     STRING,
    accuracy         FLOAT,
    auc              FLOAT,
    promoted         BOOLEAN,
    promotion_note   STRING
);

-- Step 3: Grant FINTECH_ML_ENGINEER everything it needs -- ONE role,
-- ONE consistent set of grants, nothing split across roles.
GRANT USAGE ON SCHEMA FINTECH_PROD.ML TO ROLE FINTECH_ML_ENGINEER;
 GRANT CREATE TABLE, CREATE VIEW, CREATE MODEL, CREATE PROCEDURE ON SCHEMA FINTECH_PROD.ML TO ROLE FINTECH_ML_ENGINEER;
GRANT SELECT, INSERT ON TABLE FINTECH_PROD.ML.CHURN_MODEL_TRAINING_LOG TO ROLE FINTECH_ML_ENGINEER;
GRANT USAGE ON WAREHOUSE ML_WH TO ROLE FINTECH_ML_ENGINEER;

GRANT USAGE ON SCHEMA FINTECH_PROD.MARTS TO ROLE FINTECH_ML_ENGINEER;
GRANT SELECT ON TABLE FINTECH_PROD.MARTS.MART_CUSTOMER_360 TO ROLE FINTECH_ML_ENGINEER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA FINTECH_PROD.MARTS TO ROLE FINTECH_ML_ENGINEER;

-- ============================================================
-- Step 4: Create TRAIN_CHURN_MODEL
-- ============================================================
USE ROLE FINTECH_ML_ENGINEER;
USE WAREHOUSE ML_WH;
USE DATABASE FINTECH_PROD;
USE SCHEMA ML;

CREATE OR REPLACE PROCEDURE TRAIN_CHURN_MODEL()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-ml-python', 'snowflake-snowpark-python', 'scikit-learn')
HANDLER = 'train_churn_model'
AS
$$
from datetime import datetime, timezone
from snowflake.snowpark import Session
from snowflake.snowpark import functions as F
from snowflake.ml.modeling.xgboost import XGBClassifier
from snowflake.ml.modeling.metrics import accuracy_score
from sklearn.metrics import roc_auc_score as sklearn_roc_auc_score
from snowflake.ml.registry import Registry

FEATURE_COLS = [
    "TOTAL_ACCOUNTS", "OPEN_ACCOUNTS", "FROZEN_ACCOUNTS", "CLOSED_ACCOUNTS",
    "TOTAL_BALANCE", "AVG_ACCOUNT_BALANCE", "TRANSACTION_COUNT_ALL_TIME",
    "TRANSACTION_AMOUNT_30D", "FAILED_PAYMENT_COUNT", "SUPPORT_CASE_COUNT",
    "HIGH_PRIORITY_CASE_COUNT", "OPEN_CASE_COUNT", "ACCOUNT_AGE_DAYS",
    "HAS_RISK_FLAG",
]
LABEL_COL = "CHURN_LABEL"


def train_churn_model(session: Session) -> str:
    df = session.table("FINTECH_PROD.MARTS.MART_CUSTOMER_360")
    df = df.with_column(
        LABEL_COL,
        F.when(F.col("CUSTOMER_STATUS").isin(["DORMANT", "CLOSED"]), F.lit(1)).otherwise(F.lit(0)),
    )
    df = df.dropna(subset=FEATURE_COLS + [LABEL_COL])
    train_df, test_df = df.random_split(weights=[0.8, 0.2], seed=42)

    model = XGBClassifier(
        input_cols=FEATURE_COLS,
        label_cols=[LABEL_COL],
        output_cols=["CHURN_PREDICTION"],
        max_depth=6,
        n_estimators=100,
    )
    model.fit(train_df)

    predictions = model.predict(test_df)
    accuracy = accuracy_score(df=predictions, y_true_col_names=LABEL_COL, y_pred_col_names="CHURN_PREDICTION")

    proba_predictions = model.predict_proba(test_df, output_cols_prefix="CHURN_PROB_")
    prob_col = [c for c in proba_predictions.columns if c.replace('"', '').upper().endswith("_1")][0]
    scored_pdf = proba_predictions.select(
        F.col(LABEL_COL).alias("Y_TRUE"),
        F.col(prob_col).alias("Y_SCORE"),
    ).to_pandas()
    auc = sklearn_roc_auc_score(scored_pdf["Y_TRUE"], scored_pdf["Y_SCORE"])

    version_name = "v_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    registry = Registry(session=session, database_name="FINTECH_PROD", schema_name="ML")
    new_model_version = registry.log_model(
        model,
        model_name="CUSTOMER_CHURN_MODEL",
        version_name=version_name,
        comment="XGBoost churn classifier trained on MART_CUSTOMER_360",
        metrics={"accuracy": float(accuracy), "auc": float(auc)},
    )

    registered_model = registry.get_model("CUSTOMER_CHURN_MODEL")
    current_default = None
    try:
        current_default = registered_model.default
    except Exception:
        pass

    promoted = False
    if current_default is None:
        registered_model.default = new_model_version
        promoted = True
        promotion_note = "No prior model existed -- promoted as the new default."
    else:
        current_metrics = current_default.show_metrics()
        current_auc = current_metrics.get("auc", 0.0)
        if auc > current_auc:
            registered_model.default = new_model_version
            promoted = True
            promotion_note = f"PROMOTED: new AUC {auc:.4f} beat current default's AUC {current_auc:.4f}."
        else:
            promotion_note = (
                f"NOT PROMOTED: new AUC {auc:.4f} did not beat current default's "
                f"AUC {current_auc:.4f}. Current default ({current_default.version_name}) "
                f"remains in production; this run's model ({version_name}) was still "
                f"saved to the registry for reference/comparison, just not deployed."
            )

    session.sql(
        "INSERT INTO FINTECH_PROD.ML.CHURN_MODEL_TRAINING_LOG "
        "(version_name, accuracy, auc, promoted, promotion_note) "
        "SELECT ?, ?, ?, ?, ?",
        params=[new_model_version.version_name, float(accuracy), float(auc), promoted, promotion_note],
    ).collect()

    return (
        f"Training complete. Accuracy={accuracy:.4f}, AUC={auc:.4f}, "
        f"version={new_model_version.version_name}. {promotion_note}"
    )
$$;

-- ============================================================
-- Step 5: Create SCORE_CHURN_MODEL
-- ============================================================
CREATE OR REPLACE PROCEDURE SCORE_CHURN_MODEL()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-ml-python', 'snowflake-snowpark-python')
HANDLER = 'score_churn_model'
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark import functions as F
from snowflake.ml.registry import Registry

FEATURE_COLS = [
    "TOTAL_ACCOUNTS", "OPEN_ACCOUNTS", "FROZEN_ACCOUNTS", "CLOSED_ACCOUNTS",
    "TOTAL_BALANCE", "AVG_ACCOUNT_BALANCE", "TRANSACTION_COUNT_ALL_TIME",
    "TRANSACTION_AMOUNT_30D", "FAILED_PAYMENT_COUNT", "SUPPORT_CASE_COUNT",
    "HIGH_PRIORITY_CASE_COUNT", "OPEN_CASE_COUNT", "ACCOUNT_AGE_DAYS",
    "HAS_RISK_FLAG",
]


def score_churn_model(session: Session) -> str:
    registry = Registry(session=session, database_name="FINTECH_PROD", schema_name="ML")
    model_version = registry.get_model("CUSTOMER_CHURN_MODEL").default

    customers = session.table("FINTECH_PROD.MARTS.MART_CUSTOMER_360")
    customers = customers.dropna(subset=FEATURE_COLS)

    scored = model_version.run(customers, function_name="predict_proba")
    # model_version.run() names probability columns PREDICT_PROBA_<class>; locate the positive-class column
    prob_col = [c for c in scored.columns if c.replace('"', '').upper().endswith("_1")][0]
    scored = scored.select(
        "CUSTOMER_ID",
        F.col(prob_col).alias("CHURN_PROBABILITY"),
        F.current_timestamp().alias("SCORED_AT"),
    )
    scored.write.save_as_table("FINTECH_PROD.ML.CHURN_PREDICTIONS", mode="overwrite")

    row_count = scored.count()
    return f"Scored {row_count} customers using model version {model_version.version_name}."
$$;

-- ============================================================
-- Step 6: Run it
-- ============================================================
CALL TRAIN_CHURN_MODEL();
CALL SCORE_CHURN_MODEL();

SELECT * FROM FINTECH_PROD.ML.CHURN_PREDICTIONS ORDER BY CHURN_PROBABILITY DESC LIMIT 20;
SELECT * FROM FINTECH_PROD.ML.CHURN_MODEL_TRAINING_LOG ORDER BY trained_at DESC;

USE ROLE ACCOUNTADMIN;
GRANT CREATE STREAM ON SCHEMA FINTECH_PROD.MARTS TO ROLE FINTECH_ML_ENGINEER;
GRANT CREATE TASK ON SCHEMA FINTECH_PROD.ML TO ROLE FINTECH_ML_ENGINEER;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE FINTECH_ML_ENGINEER;

USE ROLE FINTECH_ML_ENGINEER;

-- Tracks new rows appearing in the mart -- fires only when dbt
-- actually rebuilds it with fresh data, not on some blind timer
CREATE STREAM IF NOT EXISTS FINTECH_PROD.MARTS.MART_CUSTOMER_360_STREAM
    ON TABLE FINTECH_PROD.MARTS.MART_CUSTOMER_360
    APPEND_ONLY = TRUE;

-- Runs train+score, but only when the stream shows the mart genuinely
-- changed -- otherwise this is a free no-op, no wasted compute
CREATE OR REPLACE TASK FINTECH_PROD.ML.CHURN_RETRAIN_TASK
    WAREHOUSE = ML_WH
    SCHEDULE = '5 MINUTE'  -- checks every 5 min; only DOES anything if new data landed
WHEN
    SYSTEM$STREAM_HAS_DATA('FINTECH_PROD.MARTS.MART_CUSTOMER_360_STREAM')
AS
BEGIN
    CALL FINTECH_PROD.ML.TRAIN_CHURN_MODEL();
    CALL FINTECH_PROD.ML.SCORE_CHURN_MODEL();
END;

ALTER TASK FINTECH_PROD.ML.CHURN_RETRAIN_TASK RESUME;


-- did we correctly flag this person before they churned
CREATE OR REPLACE VIEW FINTECH_PROD.ML.CHURN_TRACKING AS
SELECT
    c.customer_id,
    c.customer_status,
    c.customer_status IN ('DORMANT', 'CLOSED') AS actually_churned,
    p.churn_probability,
    p.scored_at,
    CASE
        WHEN c.customer_status IN ('DORMANT', 'CLOSED') AND p.churn_probability >= 0.5 THEN 'CORRECTLY FLAGGED'
        WHEN c.customer_status IN ('DORMANT', 'CLOSED') AND p.churn_probability < 0.5 THEN 'MISSED (false negative)'
        WHEN c.customer_status = 'ACTIVE' AND p.churn_probability >= 0.5 THEN 'FALSE ALARM (false positive)'
        ELSE 'CORRECTLY NOT FLAGGED'
    END AS tracking_outcome
FROM FINTECH_PROD.MARTS.MART_CUSTOMER_360 c
LEFT JOIN FINTECH_PROD.ML.CHURN_PREDICTIONS p ON c.customer_id = p.customer_id;

-- who's actually churned right now
SELECT COUNT(*) FROM FINTECH_PROD.ML.CHURN_TRACKING WHERE actually_churned;

-- how well is the model actually doing, in plain business terms
SELECT tracking_outcome, COUNT(*) FROM FINTECH_PROD.ML.CHURN_TRACKING GROUP BY tracking_outcome;

-- the highest-value list: currently active customers with high churn risk right now
SELECT customer_id, churn_probability
FROM FINTECH_PROD.ML.CHURN_TRACKING
WHERE customer_status = 'ACTIVE' AND churn_probability >= 0.5
ORDER BY churn_probability DESC;

