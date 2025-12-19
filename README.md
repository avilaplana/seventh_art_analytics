# movies_analytics
The intention with the project is tp exercise the Data engineering practises. These are the initial thoughts:

-  Domain model: Movies/Series.
-  Data design pattern: Medallion Architecture.
-  Orchestrator and Scheduler: Airflow
-  Cleansing, Validation and Transformation: PySpark
-  Kimball data model: DBT + Spark
-  Spark Cluster: EMR, Databricks, ... TO DECIDE
-  Data Storage: Iceberg, Snowflake, ... TO DECIDE
-  BI/Data Analytics: ... TO DECIDE

The first approach will be to provision and deploy the data platform in a local environment with docker and eventually 
the intention is to do it in the cloud.


Notes: steps to take in the medallion architecture

# Silver Layer – Table Inventory

## 1. Core Reference (Lookup / Enum tables)

These tables stabilize values that appear repeatedly and/or come from controlled vocabularies.

### 1.1 `silver.title_type`

From `title.basics.titleType`

| Column            | Notes                         |
| ----------------- | ----------------------------- |
| `title_type_id`   | Surrogate key                 |
| `title_type_code` | `movie`, `tv_series`, `short` |
| `title_type_name` | `Movie`, `TV Series`, `Short` |

---

### 1.2 `silver.genre`

From `title.basics.genres`

| Column       | Notes                         |
| ------------ | ----------------------------- |
| `genre_id`   | Surrogate key                 |
| `genre_code` | `action`, `sci_fi`, `romance` |
| `genre_name` | `Action`, `Sci-Fi`, `Romance` |

---

### 1.3 `silver.role`

From `title.principals.category`

| Column      | Notes                         |
| ----------- | ----------------------------- |
| `role_id`   | Surrogate key                 |
| `role_code` | `actor`, `director`, `writer` |
| `role_name` | `Actor`, `Director`, `Writer` |

---

## 2. Core Entities

These are your **canonical domain entities**.

---

### 2.1 `silver.title`

From `title.basics`

| Column            |
| ----------------- |
| `title_id`        |
| `title_type_id`   |
| `primary_title`   |
| `original_title`  |
| `is_adult`        |
| `start_year`      |
| `end_year`        |
| `runtime_minutes` |

✔ One row per title
✔ No genres here (many-to-many)

---

### 2.2 `silver.person`

From `name.basics`

| Column         |
| -------------- |
| `person_id`    |
| `primary_name` |
| `birth_year`   |
| `death_year`   |

✔ One row per person
✔ No titles here

---

## 3. Relationship (Bridge) Tables

These resolve many-to-many relationships explicitly.

---

### 3.1 `silver.title_genre`

From `title.basics.genres`

| Column     |
| ---------- |
| `title_id` |
| `genre_id` |

✔ One row per title-genre relationship
✔ Enables clean filtering & aggregation

---

### 3.2 `silver.title_person_role`

From `title.principals`

| Column      |
| ----------- |
| `title_id`  |
| `person_id` |
| `role_id`   |
| `ordering`  |

✔ Supports multiple roles per person per title
✔ Keeps IMDb ordering info

---

## 4. Ratings & Metrics

Still Silver — no aggregation yet.

---

### 4.1 `silver.title_rating`

From `title.ratings`

| Column           |
| ---------------- |
| `title_id`       |
| `average_rating` |
| `num_votes`      |

✔ Clean numeric types
✔ No bucketing or scoring logic

---

## 5. Episode Relationships (Optional but Recommended)

If you want full TV analytics.

---

### 5.1 `silver.episode`

From `title.episode`

| Column           |
| ---------------- |
| `episode_id`     |
| `series_id`      |
| `season_number`  |
| `episode_number` |

✔ Enables season & episode rollups
✔ Critical for TV series analysis

---

# 🧱 Final Table Count (Silver)

### **Total: 11 tables**

**Reference**

1. `silver.title_type`
2. `silver.genre`
3. `silver.role`

**Core**
4. `silver.title`
5. `silver.person`

**Relationships**
6. `silver.title_genre`
7. `silver.title_person_role`

**Metrics**
8. `silver.title_rating`

**TV Structure**
9. `silver.episode`

(Optional future)
10. `silver.profession`
11. `silver.person_profession`

---

## 🧠 Why this list works

✔ No multi-value columns
✔ Stable joins
✔ Easy to extend
✔ Gold layer becomes trivial
✔ BI tools love it

This is a **production-grade Silver model**, not a toy.
