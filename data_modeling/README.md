# Data Modeling

This folder contains the data modeling artifacts for the ecommerce analytics project, including both the **normalized relational model** and the **dimensional (star/galaxy) schema** used to power downstream analysis and dashboards.

---

## Query Performance Result (updated on 04.12.2026)

The same 9 analytical views were rebuilt on top of a dimensional Star Schema database and validated against the original outputs.

### 📌 Runtime Comparison

| Version | Total Runtime |
|--------|--------------:|
| Original model | 0.805574 s |
| Star Schema model | 0.536462 s |

### Result

- Absolute improvement: `0.269112 s`
- Relative improvement: `33.4% faster`

The optimized Star Schema version preserved the same analytical results while reducing the total execution time across all 9 views.

---

## Logical ERD

### Overview

This project demonstrates the end-to-end data modeling process:

1. **Normalized Model (Source Layer)**
   - Designed using relational principles (3NF)
   - Captures raw business entities: users, orders, order items, products, events, exposures
   - Ensures data integrity and minimal redundancy

2. **Dimensional Model (Analytics Layer)**
   - Re-modeled into a **galaxy schema** with multiple fact tables and shared dimensions
   - Optimized for analytical queries and dashboard performance
   - Supports business use cases such as revenue analysis, product performance, and A/B testing

---

### Dimensional Model Design

#### Fact Tables

- **fact_orders**  
  - Grain: one row per order  
  - Measures: revenue  
  - Used for GMV, AOV, and order-level metrics  

- **fact_order_items**  
  - Grain: one row per order item  
  - Measures: qty, line_amount  
  - Used for SKU/category analysis and basket insights  
  - Includes `user_key` (denormalized) to improve query performance for drill-down analysis  

- **fact_exposures** *(factless fact table)*  
  - Grain: one row per exposure event  
  - No explicit measures; supports COUNT-based metrics  
  - Used for A/B test analysis and user segmentation  

---

#### Dimension Tables

- **dim_users**  
  - User attributes including signup date and acquisition channel  

- **dim_product**  
  - Product attributes such as SKU, category, and price  

- **dim_date**  
  - Calendar dimension enabling time-based aggregation (year, quarter, month, weekday)  

---

### Key Design Decisions

- **Grain-first modeling**  
  Each fact table is defined with a clear and consistent grain before adding attributes or measures.

- **Separation of concerns**  
  Order-level and item-level data are modeled in separate fact tables to avoid aggregation errors.

- **Controlled denormalization**  
  Selected fields (e.g., `user_key` in fact_order_items) are intentionally duplicated to reduce join complexity and improve performance.

- **Factless fact table for experiments**  
  A/B test exposures are modeled without measures, enabling flexible metric computation via joins with other fact tables.

- **Shared dimensions (conformed dimensions)**  
  All fact tables connect to a consistent set of dimensions, enabling cross-domain analysis (e.g., experiment → orders).

---

### Analytical Capabilities Enabled

This model supports:

- Revenue and order trend analysis
- Product/category performance breakdown
- Cohort and retention analysis
- A/B test uplift measurement (conversion, GMV, AOV)
- Basket pair (co-occurrence) analysis via self-join on fact_order_items

---

### Summary

This data model reflects a transition from **normalized transactional design** to **analytics-optimized dimensional modeling**, balancing:

- Data integrity  
- Query performance  
- Analytical flexibility  

It is designed to closely mirror real-world data warehouse practices used in production environments.

**Copyright @Iris Xia**
