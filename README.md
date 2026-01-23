# E-commerce Executive Summary Dashboard (Tableau)

## Project Overview
This project focuses on building an **executive-level Tableau dashboard** that consolidates key e-commerce performance insights across **monthly trends, marketing channels, and product categories**.

The goal is not just visualization, but **business storytelling** — translating structured data into clear, decision-oriented narratives suitable for leadership review.

---

## Business Questions
The dashboard was designed to answer several high-level questions commonly faced by e-commerce decision-makers:

- How do overall GMV and user activity trend across months?
- Which marketing channels are driving revenue, and which channels show higher sensitivity to personalized recommendation strategies?
- Do observed performance differences across channels stem from changes in conversion rate or average order value (AOV)?
- How do product categories and cohort-level patterns complement experimentation insights to inform broader growth strategy?

---

## Data Generation & Preparation
To simulate a realistic analytics workflow, the dataset was **synthetically generated** and processed through a structured SQL pipeline:

- Created an **SQLite database** using AI-assisted SQL scripts
- Designed multiple SQL **views** to:
  - Aggregate GMV, users, and order-level metrics
  - Structure data by time, channel, and product category
  - Prepare cohort- and product-level summaries for analysis
- Performed data exploration and metric validation at the SQL layer
- Exported cleaned, analysis-ready tables as CSV files

> All calculations and metric logic were handled **upstream in SQL**, rather than directly inside Tableau, to reflect real-world analytics best practices.

---

## Visualization & Dashboard Design
The final dashboard was built in **Tableau**, with emphasis on:

- Executive-friendly KPI design
- Clean layout and logical information hierarchy
- Consistent color encoding across metrics
- Clear separation between trend analysis and categorical breakdowns

Tableau was used purely for **visual storytelling**, not raw computation.

---

## Key Insights
Some notable insights surfaced in the dashboard include:

- **GMV peaked at $1.28M with 45K users**, indicating strong summer seasonality
- **Paid Ads** remained the top revenue driver, while **Referral channels** showed emerging growth potential  
  (ROI comparison across channels remains a key next step for strategic decision-making)
- **Home** and **Sports** categories consistently led performance, each maintaining over **20% GMV share** across quarters

---

## Tools & Technologies
- **SQL / SQLite**: data generation, transformation, and view creation
- **Tableau**: dashboard design and executive-level storytelling

---

## Project Takeaways
This project helped bridge **business consulting thinking and data analytics execution**, moving from raw metrics and structured queries to an executive-ready dashboard that supports strategic discussion.

---

## Links
- Tableau Dashboard: *https://public.tableau.com/app/profile/iris.xia/vizzes*
