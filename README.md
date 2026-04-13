# E-commerce Analytics System: Data Pipeline, Modeling & Dashboard

## 📌 Overview
This project builds an end-to-end analytics workflow for an e-commerce dataset, transforming raw data into decision-ready insights.

It answers key business questions around revenue trends, marketing channel performance, product mix, and experimentation outcomes while leveraging a dimensional data model and SQL-based query optimizations to support scalable analysis.

---

## 📊 Dashboard
The Tableau dashboard provides:

- Revenue (GMV), users, conversion, and ARPU trends  
- Channel-level performance and A/B test uplift  
- Product category contribution and cohort behavior  

Designed for executive-level decision-making with a clear KPI structure and drill-down capabilities.

---

## 🧱 Data Modeling & Optimization
- Transformed a normalized (3NF) schema into a star-schema analytics layer  
- Built fact tables (`orders`, `order_items`, `exposures`) and shared dimensions  
- Reduced query complexity and improved query performance by **33.4%**  
- Enabled efficient aggregation and multi-dimensional analysis  

### Performance Note

A comparison was conducted between normalized and star schema designs under both small-scale and 100× scaled datasets in SQLite.

- At small scale, star schema queries were faster
- At 100× scale, star schema became ~11% slower than the normalized model

This highlights that performance depends on the database engine and workload characteristics, rather than schema design alone.

---

## ⚙️ Data Pipeline
- Built using **Python + SQL (SQLite)** for data ingestion (CSV to SQLite) and transformation  
- Structured logic using **CTEs and window functions**  
- Automated pipeline execution via **cron** for reproducible refreshes  

---

## 📈 Key Insights
- GMV peaked at ~$1.28M with ~45K users, showing seasonal trends  
- Paid Ads drove the majority of revenue, while Referral showed growth potential  
- Home and Sports categories consistently contributed >20% of GMV  

---

## 🚀 Next Steps
- Scale dataset to 10M+ rows for stress testing  
- Benchmark performance across larger datasets  
- Explore migration to cloud data warehouses (e.g., Snowflake / BigQuery)  

---

## 🔗 Links
- Tableau Dashboard: https://public.tableau.com/app/profile/iris.xia/vizzes
  
---
**copyright @Iris Xia**
