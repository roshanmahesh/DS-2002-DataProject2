# DS-2002 Data Project 2 – Hotel Booking Data Lakehouse
**Author:** Roshan Mahesh (dmg4fy)  
**Course:** DS-2002 – Data Science Systems  
**University:** University of Virginia  
**Date:** May 2026

---

## Overview

This project builds upon Data Project 1 (Hotel Booking ETL Pipeline) and implements a full **dimensional Data Lakehouse** using **Azure Databricks** with **Delta Tables** and **Spark Structured Streaming**. It demonstrates data extraction from multiple source types, transformation via the Bronze/Silver/Gold medallion architecture, and analytical querying against a dimensional star schema.

**Business Process:** Hotel booking and reservation management across two Portuguese hotels (City Hotel and Resort Hotel), sourced from 119,390 real booking records spanning July 2015 – August 2017 (Antonio, Almeida & Nunes, 2019).

---

## Architecture

```
Source 1: DBFS CSV          Source 2: MongoDB Atlas       Source 3: DBFS JSON (Streaming)
(hotels.csv,                (guest_profiles collection     (fact_stream_2015/2016/2017.json)
 room_types.csv)             → dim_guest_origin)                      │
        │                           │                                  │
        ▼                           ▼                                  ▼
   Batch ETL                   Batch ETL                    Spark Structured Streaming
        │                           │                                  │
        ▼                           ▼                                  ▼
  dim_hotel               dim_guest_origin               Bronze Delta Table
  dim_room_type           dim_date (generated)           (raw streaming fact rows)
        │                           │                                  │
        └───────────────────────────┴──────────────────────────────────┘
                                    │
                                    ▼
                          Silver Delta Table
                     (fact + all dims joined)
                                    │
                                    ▼
                          Gold Delta Tables
                     (pre-aggregated analytics)
                                    │
                                    ▼
                        Business Value SQL Queries
```

### Medallion Architecture
| Layer | Table | Description |
|-------|-------|-------------|
| **Bronze** | `bronze_fact_bookings` | Raw streaming fact rows ingested from 3 JSON files |
| **Silver** | `silver_fact_bookings_enriched` | Bronze joined with all 4 dimension tables |
| **Gold** | `gold_revenue_by_hotel_year` | Revenue & cancellations by hotel and year |
| **Gold** | `gold_top_countries_revenue` | Top 15 guest countries by revenue |
| **Gold** | `gold_room_type_performance` | Room type revenue and weekend vs weekday split |

---

## Star Schema

```
                        dim_date
                           │
  dim_hotel ──── fact_bookings ──── dim_guest_origin
                           │
                     dim_room_type
```

### Dimension Tables
| Table | Source | Rows | Description |
|-------|--------|------|-------------|
| `dim_date` | Generated | 793 | Every date from July 2015 – Aug 2017 |
| `dim_hotel` | DBFS CSV | 2 | Hotel name and type (Resort/City) |
| `dim_guest_origin` | MongoDB Atlas | 177 | Country-level guest profiles aggregated from 119,390 documents |
| `dim_room_type` | DBFS CSV | 10 | Room codes, names, and rate tiers |

### Fact Table
| Table | Source | Rows | Description |
|-------|--------|------|-------------|
| `fact_bookings` | DBFS JSON (streaming) | 119,390 | One row per booking with all surrogate keys and measures |

---

## Data Sources

### Source 1 – Structured Reference Data (DBFS CSV)
`hotels.csv` and `room_types.csv` are structured reference files hosted on the Databricks File System (DBFS), representing the relational source originally created in the Project 1 MySQL database (`hotel_reference`). These are loaded via batch ETL into `dim_hotel` and `dim_room_type`.

### Source 2 – NoSQL Database (MongoDB Atlas)
The `guest_profiles` collection in MongoDB Atlas contains 119,390 documents with per-booking guest metadata (country, market segment, cancellation history). These are extracted, aggregated by country, and loaded into `dim_guest_origin` (177 rows).

### Source 3 – Streaming File Source (DBFS JSON)
The booking transaction data is segmented into 3 JSON files by arrival year to simulate real-time streaming intervals:
- `fact_stream_2015.json` — 21,996 rows (Interval 1)
- `fact_stream_2016.json` — 56,707 rows (Interval 2)
- `fact_stream_2017.json` — 40,687 rows (Interval 3)

These are read via **Spark Structured Streaming** with AutoLoader (`cloudFiles`) into the Bronze Delta table.

---

## Data Integration Patterns

| Pattern | Where Used |
|---------|-----------|
| **ELT** | MongoDB → pandas aggregation → Spark Delta |
| **Batch ETL** | DBFS CSV → dim_hotel, dim_room_type Delta tables |
| **Structured Streaming (mini-batch)** | 3 JSON files → Bronze Delta via AutoLoader |
| **Incremental Load** | Append-mode Delta write in Section 3.4 |
| **Lambda Architecture** | Batch dims (static) + streaming fact (near real-time) |

---

## Repository Contents

| File | Description |
|------|-------------|
| `DS_2002_DataProject2_HotelBooking.py` | Full Databricks notebook (import as .py into Databricks) |
| `hotels.csv` | Structured reference data – 2 hotel records |
| `room_types.csv` | Structured reference data – 10 room type records |
| `fact_stream_2015.json` | Streaming interval 1 – 21,996 booking records |
| `fact_stream_2016.json` | Streaming interval 2 – 56,707 booking records |
| `fact_stream_2017.json` | Streaming interval 3 – 40,687 booking records |

---

## Notebook Structure

| Section | Description |
|---------|-------------|
| 1 | Setup, library installation, configuration |
| 2 | DBFS file verification |
| 3 | Batch dimension loading (dim_date, dim_hotel, dim_room_type, dim_guest_origin) + incremental load demo |
| 4 | Bronze layer – Spark Structured Streaming via AutoLoader (3 intervals) |
| 5 | Silver layer – Bronze joined with all 4 dimension tables |
| 6 | Gold layer – 3 pre-aggregated analytical tables |
| 7 | Business value SQL queries (5 queries) |
| 8 | Delta table registry and pipeline summary |

---

## Business Value Queries

| Query | Business Question |
|-------|------------------|
| 1 | Revenue & cancellation trend by hotel and year |
| 2 | Top 10 guest countries by total revenue |
| 3 | Room type revenue vs weekend proportion |
| 4 | Quarterly cancellation rates by top 5 countries |
| 5 | Monthly booking volume and average daily rate |

---

## How to Run

1. Import `DS_2002_DataProject2_HotelBooking.py` into your Databricks workspace
2. Upload all CSV and JSON files to DBFS at `/FileStore/hotel_streaming/`
3. Install `pymongo` on your cluster via Libraries tab
4. Update the MongoDB Atlas URI in Section 1.1
5. Attach your cluster and click **Run All**

---

## Data Source

Antonio, N., de Almeida, A., & Nunes, L. (2019). Hotel Booking Demand Datasets. *Data in Brief*, Vol. 22. Retrieved from [Kaggle](https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand).
