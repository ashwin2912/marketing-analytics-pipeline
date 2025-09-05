# Marketing Analytics Pipeline - Data Dictionary

## Dimensional Model Tables

### DIM_CUSTOMER
**Purpose:** Customer dimension containing enriched customer profiles with segmentation and lifecycle metrics

| Column Name | Data Type | Description | Business Rules |
|-------------|-----------|-------------|----------------|
| customer_id | INTEGER | Primary key - Natural customer identifier | NOT NULL, PRIMARY KEY |
| first_order_date | DATE | Customer acquisition date | NOT NULL |
| last_order_date | DATE | Most recent purchase date | NOT NULL |
| total_transactions | INTEGER | Count of all transactions | >= 0 |
| total_spent | REAL | Lifetime customer value in currency | >= 0 |
| avg_order_value | REAL | Average transaction amount | >= 0 |
| total_orders | INTEGER | Count of unique orders | >= 0 |
| first_order_cohort_month | TEXT | Acquisition cohort in YYYY-MM format | Format: YYYY-MM |
| first_order_cohort_quarter | TEXT | Acquisition quarter | Format: Q1, Q2, Q3, Q4 |
| first_order_cohort_year | INTEGER | Acquisition year | 4-digit year |
| days_since_first_order | INTEGER | Customer age in days | >= 0 |
| customer_vintage_group | TEXT | Age-based segmentation | Values: New, Established, Veteran |
| days_since_last_order | INTEGER | Recency metric | >= 0 |
| customer_segment | TEXT | Value-based segmentation | Values: High Value, Regular, At Risk, Lost |
| customer_status | TEXT | Activity status | Values: Active, Dormant, Lost |
| created_at | TIMESTAMP | Record creation timestamp | DEFAULT CURRENT_TIMESTAMP |
| updated_at | TIMESTAMP | Last modification timestamp | DEFAULT CURRENT_TIMESTAMP |

### DIM_DATE
**Purpose:** Date dimension providing rich temporal hierarchy and business calendar attributes

| Column Name | Data Type | Description | Business Rules |
|-------------|-----------|-------------|----------------|
| date_id | INTEGER | Primary key in YYYYMMDD format | NOT NULL, PRIMARY KEY |
| full_date | DATE | Natural date value | NOT NULL |
| year | INTEGER | 4-digit year | Range: 1900-2100 |
| quarter | INTEGER | Quarter number | Range: 1-4 |
| month | INTEGER | Month number | Range: 1-12 |
| month_name | TEXT | Full month name | Values: January, February, etc. |
| month_abbr | TEXT | 3-letter month abbreviation | Values: Jan, Feb, etc. |
| week_of_year | INTEGER | Week number within year | Range: 1-53 |
| day_of_year | INTEGER | Day number within year | Range: 1-366 |
| day_of_month | INTEGER | Day within month | Range: 1-31 |
| day_of_week | INTEGER | Day of week number | Range: 1-7 (1=Sunday) |
| day_name | TEXT | Full day name | Values: Monday, Tuesday, etc. |
| day_abbr | TEXT | 3-letter day abbreviation | Values: Mon, Tue, etc. |
| is_weekend | INTEGER | Boolean flag for weekend | Values: 0, 1 |
| is_month_start | INTEGER | Boolean flag for first day of month | Values: 0, 1 |
| is_month_end | INTEGER | Boolean flag for last day of month | Values: 0, 1 |
| is_quarter_start | INTEGER | Boolean flag for first day of quarter | Values: 0, 1 |
| is_quarter_end | INTEGER | Boolean flag for last day of quarter | Values: 0, 1 |
| is_year_start | INTEGER | Boolean flag for January 1st | Values: 0, 1 |
| is_year_end | INTEGER | Boolean flag for December 31st | Values: 0, 1 |
| date_string | TEXT | Date in string format for display | Format: YYYY-MM-DD |
| month_year | TEXT | Month-year format for reporting | Format: Mon YYYY |
| quarter_year | TEXT | Quarter-year format for reporting | Format: Q1 YYYY |
| created_at | TIMESTAMP | Record creation timestamp | DEFAULT CURRENT_TIMESTAMP |

### DIM_ORDER
**Purpose:** Order dimension providing detailed order context and customer journey insights

| Column Name | Data Type | Description | Business Rules |
|-------------|-----------|-------------|----------------|
| order_id | INTEGER | Primary key - Natural order identifier | NOT NULL, PRIMARY KEY |
| customer_id | INTEGER | Customer relationship | NOT NULL, FK to dim_customer |
| order_date | DATE | Order date | NOT NULL |
| date_id | INTEGER | Foreign key to dim_date | NOT NULL, FK to dim_date |
| order_amount | REAL | Order total value | >= 0 |
| order_year | INTEGER | Order year for partitioning | 4-digit year |
| order_month | INTEGER | Order month | Range: 1-12 |
| order_quarter | INTEGER | Order quarter | Range: 1-4 |
| order_day_of_week | INTEGER | Day of week analysis | Range: 1-7 |
| order_day_name | TEXT | Day name for reporting | Values: Monday, Tuesday, etc. |
| is_weekend_order | INTEGER | Weekend/weekday flag | Values: 0, 1 |
| customer_order_sequence | INTEGER | Order sequence number for customer | >= 1 |
| is_first_order | INTEGER | First order flag | Values: 0, 1 |
| days_since_customer_first_order | INTEGER | Days from customer acquisition | >= 0 |
| days_since_previous_order | INTEGER | Inter-purchase interval | >= 0 |
| order_amount_quartile | TEXT | Order value quartile classification | Values: Q1, Q2, Q3, Q4 |
| created_at | TIMESTAMP | Record creation timestamp | DEFAULT CURRENT_TIMESTAMP |

### FACT_SALES
**Purpose:** Sales fact table storing transactional metrics with foreign keys to all dimensions

| Column Name | Data Type | Description | Business Rules |
|-------------|-----------|-------------|----------------|
| customer_id | INTEGER | Foreign key to dim_customer | NOT NULL, FK to dim_customer |
| order_id | INTEGER | Foreign key to dim_order | NOT NULL, FK to dim_order |
| date_id | INTEGER | Foreign key to dim_date | NOT NULL, FK to dim_date |
| sales_amount | REAL | Transaction amount (additive measure) | >= 0 |
| transaction_count | INTEGER | Count of transactions (additive measure) | >= 1 |
| created_at | TIMESTAMP | Record creation timestamp | DEFAULT CURRENT_TIMESTAMP |
| | | **Composite Primary Key:** (customer_id, order_id, date_id) | |

## Business Analytics Tables

### MONTHLY_METRICS
**Purpose:** Pre-aggregated monthly business performance metrics

| Column Name | Data Type | Description | Business Rules |
|-------------|-----------|-------------|----------------|
| period_month | TEXT | YYYY-MM format | PRIMARY KEY, Format: YYYY-MM |
| total_sales | REAL | Monthly revenue | >= 0 |
| avg_order_value | REAL | Average order value | >= 0 |
| total_transactions | INTEGER | Transaction count | >= 0 |
| total_orders | INTEGER | Unique order count | >= 0 |
| unique_customers | INTEGER | Active customer count | >= 0 |
| purchase_frequency | REAL | Orders per customer | >= 0 |
| created_at | TIMESTAMP | Creation timestamp | DEFAULT CURRENT_TIMESTAMP |

### COHORT_ANALYSIS
**Purpose:** Customer retention analysis by acquisition cohort

| Column Name | Data Type | Description | Business Rules |
|-------------|-----------|-------------|----------------|
| cohort_month | TEXT | Acquisition month (YYYY-MM) | NOT NULL |
| activity_month | TEXT | Activity measurement month | NOT NULL |
| months_since_acquisition | INTEGER | Time since first purchase | >= 0 |
| cohort_size | INTEGER | Total customers in cohort | > 0 |
| active_customers | INTEGER | Active customers in period | >= 0 |
| retention_rate_percent | REAL | Calculated retention rate | 0-100 |
| total_sales | REAL | Cohort revenue in period | >= 0 |
| avg_order_value | REAL | Average order value for cohort | >= 0 |
| | | **Primary Key:** (cohort_month, activity_month) | |

### CUMULATIVE_RETENTION_ANALYSIS
**Purpose:** Long-term retention measurement across 3, 12, and 18-month windows

| Column Name | Data Type | Description | Business Rules |
|-------------|-----------|-------------|----------------|
| cohort_month | TEXT | Acquisition month (YYYY-MM) | NOT NULL |
| retention_window_months | INTEGER | Retention window | Values: 3, 12, 18 |
| cohort_size | INTEGER | Total customers in cohort | >= 10 (statistical significance) |
| active_customers | INTEGER | Active customers in window | >= 0 |
| cumulative_retention_rate | REAL | Cumulative retention rate | 0-100 |
| avg_purchase_frequency | REAL | Average orders per active customer | >= 0 |
| total_revenue | REAL | Total revenue in window | >= 0 |
| avg_customer_value | REAL | Average revenue per active customer | >= 0 |
| created_at | TIMESTAMP | Creation timestamp | DEFAULT CURRENT_TIMESTAMP |
| | | **Primary Key:** (cohort_month, retention_window_months) | |

### CUSTOMER_LTV_ANALYSIS
**Purpose:** Customer lifetime value and churn risk scoring

| Column Name | Data Type | Description | Business Rules |
|-------------|-----------|-------------|----------------|
| customer_id | INTEGER | Customer identifier | PRIMARY KEY |
| acquisition_cohort | TEXT | Acquisition month cohort | Format: YYYY-MM |
| customer_segment | TEXT | Value-based segment | Values: High Value, Regular, At Risk, Lost |
| total_orders | INTEGER | Lifetime order count | >= 1 |
| total_spent | REAL | Customer lifetime value | >= 0 |
| avg_order_value | REAL | Average order value | >= 0 |
| days_active | INTEGER | Days since acquisition | >= 0 |
| predicted_ltv_score | INTEGER | ML-based LTV prediction | Range: 1-5 |
| churn_risk_score | REAL | Churn probability | Range: 0-1 |

### CUSTOMER_SEGMENTATION
**Purpose:** RFM customer segmentation analysis

| Column Name | Data Type | Description | Business Rules |
|-------------|-----------|-------------|----------------|
| customer_id | INTEGER | Customer identifier | PRIMARY KEY |
| recency_score | INTEGER | Recency score | Range: 1-5 |
| frequency_score | INTEGER | Frequency score | Range: 1-5 |
| monetary_score | INTEGER | Monetary score | Range: 1-5 |
| rfm_segment | TEXT | RFM segment classification | Values: Champions, Loyal Customers, New Customers, At Risk, Cannot Lose Them, Lost Customers, Others |
| segment_description | TEXT | Segment description | NOT NULL |
| recommended_strategy | TEXT | Marketing strategy recommendation | NOT NULL |
| created_at | TIMESTAMP | Creation timestamp | DEFAULT CURRENT_TIMESTAMP |

### SEASONAL_TRENDS
**Purpose:** Seasonal performance analysis for planning

| Column Name | Data Type | Description | Business Rules |
|-------------|-----------|-------------|----------------|
| period_type | TEXT | Period type | Values: monthly, quarterly |
| period_value | TEXT | Period identifier | Format: 01-12 for monthly, Q1-Q4 for quarterly |
| avg_sales | REAL | Average sales for period | >= 0 |
| avg_orders | INTEGER | Average orders for period | >= 0 |
| avg_customers | INTEGER | Average customers for period | >= 0 |
| seasonal_index | REAL | Performance vs average | > 0 |
| trend_direction | TEXT | Trend classification | Values: strong_positive, positive, stable, negative, strong_negative |
| created_at | TIMESTAMP | Creation timestamp | DEFAULT CURRENT_TIMESTAMP |
| | | **Primary Key:** (period_type, period_value) | |

### CAMPAIGN_TARGETS
**Purpose:** Marketing campaign targeting recommendations

| Column Name | Data Type | Description | Business Rules |
|-------------|-----------|-------------|----------------|
| customer_id | INTEGER | Target customer | NOT NULL |
| campaign_type | TEXT | Campaign category | Values: Early Engagement, Re-activation, Win-back, Final Push, Long-term Win-back |
| priority_level | INTEGER | Priority ranking | Range: 1-5 |
| estimated_value | REAL | Expected campaign value | >= 0 |
| days_since_last_order | INTEGER | Recency metric | >= 0 |
| recommended_action | TEXT | Specific action recommendation | NOT NULL |
| created_at | TIMESTAMP | Creation timestamp | DEFAULT CURRENT_TIMESTAMP |
| | | **Primary Key:** (customer_id, campaign_type) | |

### BUSINESS_INSIGHTS
**Purpose:** Self-updating business intelligence with prioritized recommendations

| Column Name | Data Type | Description | Business Rules |
|-------------|-----------|-------------|----------------|
| insight_id | TEXT | Unique insight identifier | PRIMARY KEY |
| insight_type | TEXT | Type of insight | Values: CONVERSION, COHORT, RETENTION, CHURN_RISK, SEGMENTATION, CAMPAIGN, SEASONAL |
| insight_title | TEXT | Insight title | NOT NULL |
| insight_description | TEXT | Detailed insight description | NOT NULL |
| metric_value | REAL | Associated metric value | >= 0 |
| recommendation | TEXT | Actionable recommendation | NOT NULL |
| priority_level | INTEGER | Priority level | Range: 1-5 |
| created_at | TIMESTAMP | Creation timestamp | DEFAULT CURRENT_TIMESTAMP |

### CUSTOMER_LIFECYCLE_SNAPSHOT
**Purpose:** Real-time customer health tracking across lifecycle stages

| Column Name | Data Type | Description | Business Rules |
|-------------|-----------|-------------|----------------|
| snapshot_date | DATE | Snapshot date | NOT NULL |
| lifecycle_stage | TEXT | Customer lifecycle stage | Values: New, Active, At Risk, Inactive |
| customers | INTEGER | Number of customers in stage | >= 0 |
| share_of_base | REAL | Percentage of total customers | Range: 0-1 |
| avg_days_since_last_order | REAL | Average recency for stage | >= 0 |
| created_at | TIMESTAMP | Creation timestamp | DEFAULT CURRENT_TIMESTAMP |
| | | **Primary Key:** (snapshot_date, lifecycle_stage) | |

## Data Quality Rules

### General Rules
- All monetary values must be >= 0
- All count fields must be >= 0
- All percentage fields must be between 0-100
- All date fields must be valid dates
- Foreign key relationships must be maintained

### Business Rules
- Retention rates cannot exceed 100%
- Cumulative retention rates cannot increase over longer time windows
- RFM scores must be between 1-5
- LTV scores must be between 1-5
- Churn risk scores must be between 0-1
- Lifecycle stage shares must sum to approximately 1.0 per snapshot date

## Naming Conventions
- Table names: lowercase with underscores
- Column names: lowercase with underscores
- Primary keys: typically {table_name}_id
- Foreign keys: reference the primary key name from the parent table
- Boolean flags: is_{condition} or has_{condition}
- Date fields: {description}_date
- Timestamp fields: {description}_at