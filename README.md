
# Azure Customer 360 Sales & Revenue Lakehouse

## Project Overview
An end-to-end Azure Data Engineering portfolio project
that combines sample Salesforce Accounts, Salesforce
Opportunities, and SAP BusinessObjects sales report data
to build a unified Customer 360 analytics lakehouse.

## Technologies
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark
- Delta Lake
- Databricks SQL
- Databricks AI/BI Dashboards

## Architecture
Sample CSV Sources
→ Azure Data Factory
→ ADLS Gen2 Bronze
→ Databricks PySpark Silver
→ Gold Delta Tables
→ SQL KPIs
→ Customer 360 Dashboard

## Implementation
- Ingested 3 sample CSV datasets using Azure Data Factory.
- Created 3 cleaned Silver Delta tables.
- Joined customer, opportunity, and sales data using account_id.
- Built a Customer 360 Gold Delta table.
- Created 5 SQL business KPIs.
- Published a Databricks dashboard with KPI cards
  and a customer-wise revenue bar chart.

## Demo Results
| Metric | Value |
|---|---:|
| Customers | 4 |
| Opportunities | 4 |
| Pipeline Amount | 57,000 |
| Closed-Won Amount | 37,000 |
| Sales Revenue | 60,700 |

## Project Note
This project uses demonstration CSV data representing
Salesforce and SAP systems, not live enterprise connections.
The amounts shown are sample values, not real business revenue.
