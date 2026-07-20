CREATE OR REPLACE SEMANTIC VIEW FINTECH_ANALYTICS_VIEW
  TABLES (
    CUSTOMERS AS FINTECH_PROD.MARTS.MART_CUSTOMER_360
      PRIMARY KEY (CUSTOMER_ID)
      WITH SYNONYMS=('customer','client','accountholder')
      COMMENT='One row per customer -- balance, activity, risk, and churn signals',

    MERCHANTS AS FINTECH_PROD.MARTS.MART_MERCHANT_PERFORMANCE
      PRIMARY KEY (MERCHANT_ID)
      WITH SYNONYMS=('merchant','vendor','business')
      COMMENT='One row per merchant -- transaction volume and decline-rate risk signals',

    DAILY_KPIS AS FINTECH_PROD.MARTS.MART_DAILY_TRANSACTION_KPI
      PRIMARY KEY (TRANSACTION_DATE)
      WITH SYNONYMS=('daily transactions','transaction volume')
      COMMENT='One row per day -- overall transaction volume, value, and decline rate'
  )
  FACTS (
    CUSTOMERS.BALANCE_AMOUNT AS total_balance,
    CUSTOMERS.TRANSACTION_COUNT AS transaction_count_all_time,
    CUSTOMERS.FAILED_PAYMENTS AS failed_payment_count,
    CUSTOMERS.SUPPORT_CASES AS support_case_count,
    MERCHANTS.TRANSACTION_AMOUNT AS total_transaction_amount,
    MERCHANTS.DECLINED_TRANSACTIONS AS declined_count,
    DAILY_KPIS.DAY_TRANSACTION_AMOUNT AS total_transaction_amount,
    DAILY_KPIS.DAY_TRANSACTION_COUNT AS total_transaction_count
  )
  DIMENSIONS (
    CUSTOMERS.CUSTOMER_ID AS customer_id,
    CUSTOMERS.STATUS AS customer_status
      WITH SYNONYMS=('account status')
      COMMENT='ACTIVE, DORMANT, or CLOSED',
    CUSTOMERS.RISK_RATING AS risk_rating,
    CUSTOMERS.ENGAGEMENT AS engagement_segment
      WITH SYNONYMS=('activity level','churn risk segment')
      COMMENT='ACTIVE, AT_RISK, DORMANT, or NEVER_TRANSACTED',
    CUSTOMERS.HAS_RISK_FLAG AS has_risk_flag,
    MERCHANTS.MERCHANT_ID AS merchant_id,
    MERCHANTS.MERCHANT_NAME AS merchant_name,
    MERCHANTS.INDUSTRY AS industry,
    MERCHANTS.COUNTRY AS country,
    MERCHANTS.RISK_CATEGORY AS risk_category,
    MERCHANTS.IS_HIGH_DECLINE AS is_high_decline_merchant
      WITH SYNONYMS=('problem merchant','risky merchant'),
    DAILY_KPIS.DATE AS transaction_date
  )
  METRICS (
    CUSTOMERS.TOTAL_CUSTOMER_BALANCE AS SUM(customers.balance_amount)
      WITH SYNONYMS=('total balance','total deposits')
      COMMENT='Sum of current balances across all customers',
    CUSTOMERS.AVG_TRANSACTIONS_PER_CUSTOMER AS AVG(customers.transaction_count)
      COMMENT='Average lifetime transaction count per customer',
    CUSTOMERS.TOTAL_FAILED_PAYMENTS AS SUM(customers.failed_payments),
    CUSTOMERS.TOTAL_SUPPORT_CASES AS SUM(customers.support_cases),
    CUSTOMERS.CUSTOMER_COUNT AS COUNT(customers.customer_id)
      WITH SYNONYMS=('number of customers','customer count'),
    MERCHANTS.TOTAL_MERCHANT_VOLUME AS SUM(merchants.transaction_amount)
      WITH SYNONYMS=('total merchant sales','merchant transaction volume'),
    MERCHANTS.TOTAL_DECLINES AS SUM(merchants.declined_transactions),
    MERCHANTS.MERCHANT_COUNT AS COUNT(merchants.merchant_id),
    DAILY_KPIS.TOTAL_DAILY_VOLUME AS SUM(daily_kpis.day_transaction_amount)
      WITH SYNONYMS=('daily transaction value','daily revenue'),
    DAILY_KPIS.TOTAL_DAILY_COUNT AS SUM(daily_kpis.day_transaction_count)
  )
  COMMENT='Business-facing semantic layer over the fintech marts -- powers Cortex Analyst natural language Q&A for customer, merchant, and daily transaction analysis';
