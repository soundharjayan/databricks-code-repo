# Databricks notebook source
# MAGIC %md
# MAGIC #Enterprise Fleet Analytics Pipeline: Focuses on the business outcome (analytics) and the domain (fleet/logistics).

# COMMAND ----------

# MAGIC %md
# MAGIC ![logistics](logistics_project.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Download the data from the below gdrive and upload into the catalog
# MAGIC https://drive.google.com/drive/folders/1J3AVJIPLP7CzT15yJIpSiWXshu1iLXKn?usp=drive_link

# COMMAND ----------

# MAGIC %md
# MAGIC ##**1. Data Munging** -

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Visibily/Manually opening the file and capture couple of data patterns (Manual Exploratory Data Analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Programatically try to find couple of data patterns applying below EDA (File: logistics_source1)
# MAGIC 1. Apply inferSchema and toDF to create a DF and analyse the actual data.
# MAGIC 2. Analyse the schema, datatypes, columns etc.,
# MAGIC 3. Analyse the duplicate records count and summary of the dataframe.

# COMMAND ----------

# MAGIC %md
# MAGIC ###a. Passive Data Munging -  (File: logistics_source1  and logistics_source2)
# MAGIC Without modifying the data, identify:<br>
# MAGIC Shipment IDs that appear in both master_v1 and master_v2<br>
# MAGIC Records where:<br>
# MAGIC 1. shipment_id is non-numeric
# MAGIC 2. age is not an integer<br>
# MAGIC
# MAGIC Count rows having:
# MAGIC 3. fewer columns than expected
# MAGIC 4. more columns than expected

# COMMAND ----------

#Create a Spark Session Object

# COMMAND ----------

# MAGIC %md
# MAGIC ###**b. Active Data Munging** File: logistics_source1 and logistics_source2

# COMMAND ----------

# MAGIC %md
# MAGIC #####1.Combining Data + Schema Merging (Structuring)
# MAGIC 1. Read both files without enforcing schema
# MAGIC 2. Align them into a single canonical schema: shipment_id,
# MAGIC first_name,
# MAGIC last_name,
# MAGIC age,
# MAGIC role,
# MAGIC hub_location,
# MAGIC vehicle_type,
# MAGIC data_source
# MAGIC 3. Add data_source column with values as: system1, system2 in the respective dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Cleansing, Scrubbing: 
# MAGIC Cleansing (removal of unwanted datasets)<br>
# MAGIC 1. Mandatory Column Check - Drop any record where any of the following columns is NULL:shipment_id, role<br>
# MAGIC 2. Name Completeness Rule - Drop records where both of the following columns are NULL: first_name, last_name<br>
# MAGIC 3. Join Readiness Rule - Drop records where the join key is null: shipment_id<br>
# MAGIC
# MAGIC Scrubbing (convert raw to tidy)<br>
# MAGIC 4. Age Defaulting Rule - Fill NULL values in the age column with: -1<br>
# MAGIC 5. Vehicle Type Default Rule - Fill NULL values in the vehicle_type column with: UNKNOWN<br>
# MAGIC 6. Invalid Age Replacement - Replace the following values in age:
# MAGIC "ten" to -1
# MAGIC "" to -1<br>
# MAGIC 7. Vehicle Type Normalization - Replace inconsistent vehicle types: 
# MAGIC truck to LMV
# MAGIC bike to TwoWheeler

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Standardization, De-Duplication and Replacement / Deletion of Data to make it in a usable format

# COMMAND ----------

# MAGIC %md
# MAGIC Creating shipments Details data Dataframe creation <br>
# MAGIC 1. Create a DF by Reading Data from logistics_shipment_detail.json
# MAGIC 2. As this data is a clean json data, it doesn't require any cleansing or scrubbing.

# COMMAND ----------

# MAGIC %md
# MAGIC Standardizations:<br>
# MAGIC
# MAGIC 1. Add a column<br> 
# MAGIC Source File: DF of logistics_shipment_detail_3000.json<br>: domain as 'Logistics',  current timestamp 'ingestion_timestamp' and 'False' as 'is_expedited'
# MAGIC 2. Column Uniformity: 
# MAGIC role - Convert to lowercase<br>
# MAGIC Source File: DF of merged(logistics_source1 & logistics_source2)<br>
# MAGIC vehicle_type - Convert values to UPPERCASE<br>
# MAGIC Source Files: DF of logistics_shipment_detail_3000.json
# MAGIC hub_location - Convert values to initcap case<br>
# MAGIC Source Files: DF of merged(logistics_source1 & logistics_source2)<br>
# MAGIC 3. Format Standardization:<br>
# MAGIC Source Files: DF of logistics_shipment_detail_3000.json<br>
# MAGIC Convert shipment_date to yyyy-MM-dd<br>
# MAGIC Ensure shipment_cost has 2 decimal precision<br>
# MAGIC 4. Data Type Standardization<br>
# MAGIC Standardizing column data types to fix schema drift and enable mathematical operations.<br>
# MAGIC Source File: DF of merged(logistics_source1 & logistics_source2) <br>
# MAGIC age: Cast String to Integer<br>
# MAGIC Source File: DF of logistics_shipment_detail_3000.json<br>
# MAGIC shipment_weight_kg: Cast to Double<br>
# MAGIC Source File: DF of logistics_shipment_detail_3000.json<br>
# MAGIC is_expedited: Cast to Boolean<br>
# MAGIC 5. Naming Standardization <br>
# MAGIC Source File: DF of merged(logistics_source1 & logistics_source2)<br>
# MAGIC Rename: first_name to staff_first_name<br>
# MAGIC Rename: last_name to staff_last_name<br>
# MAGIC Rename: hub_location to origin_hub_city<br>
# MAGIC 6. Reordering columns logically in a better standard format:<br>
# MAGIC Source File: DF of Data from all 3 files<br>
# MAGIC shipment_id (Identifier), staff_first_name (Dimension)staff_last_name (Dimension), role (Dimension), origin_hub_city (Location), shipment_cost (Metric), ingestion_timestamp (Audit)

# COMMAND ----------

# MAGIC %md
# MAGIC Deduplication:
# MAGIC 1. Apply Record Level De-Duplication
# MAGIC 2. Apply Column Level De-Duplication (Primary Key Enforcement)

# COMMAND ----------

# MAGIC %md
# MAGIC ##2. Data Enrichment - Detailing of data
# MAGIC Makes your data rich and detailed <br>

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Adding of Columns (Data Enrichment)
# MAGIC *Creating new derived attributes to enhance traceability and analytical capability.*
# MAGIC
# MAGIC **1. Add Audit Timestamp (`load_dt`)**
# MAGIC Source File: DF of logistics_source1 and logistics_source2<br>
# MAGIC * **Scenario:** We need to track exactly when this record was ingested into our Data Lakehouse for auditing purposes.
# MAGIC * **Action:** Add a column `load_dt` using the function `current_timestamp()`.
# MAGIC
# MAGIC **2. Create Full Name (`full_name`)**
# MAGIC Source File: DF of logistics_source1 and logistics_source2<br>
# MAGIC * **Scenario:** The reporting dashboard requires a single field for the driver's name instead of separate columns.
# MAGIC * **Action:** Create `full_name` by concatenating `first_name` and `last_name` with a space separator.
# MAGIC * **Result:** "Rajesh" + " " + "Kumar" -> **"Rajesh Kumar"**
# MAGIC
# MAGIC **3. Define Route Segment (`route_segment`)**
# MAGIC Source File: DF of logistics_shipment_detail_3000.json<br>
# MAGIC * **Scenario:** The logistics team wants to analyze performance based on specific transport lanes (Source to Destination).
# MAGIC * **Action:** Combine `source_city` and `destination_city` with a hyphen.
# MAGIC * **Result:** "Chennai" + "-" + "Pune" -> **"Chennai-Pune"**
# MAGIC
# MAGIC **4. Generate Vehicle Identifier (`vehicle_identifier`)**
# MAGIC Source File: DF of logistics_shipment_detail_3000.json<br>
# MAGIC * **Scenario:** We need a unique tracking code that immediately tells us the vehicle type and the shipment ID.
# MAGIC * **Action:** Combine `vehicle_type` and `shipment_id` to create a composite key.
# MAGIC * **Result:** "Truck" + "_" + "500001" -> **"Truck_500001"**

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Deriving of Columns (Time Intelligence)
# MAGIC *Extracting temporal features from dates to enable period-based analysis and reporting.*<br>
# MAGIC Source File: logistics_shipment_detail_3000.json<br>
# MAGIC **1. Derive Shipment Year (`shipment_year`)**
# MAGIC * **Scenario:** Management needs an annual performance report to compare growth year-over-year.
# MAGIC * **Action:** Extract the year component from `shipment_date`.
# MAGIC * **Result:** "2024-04-23" -> **2024**
# MAGIC
# MAGIC **2. Derive Shipment Month (`shipment_month`)**
# MAGIC * **Scenario:** Analysts want to identify seasonal peaks (e.g., increased volume in December).
# MAGIC * **Action:** Extract the month component from `shipment_date`.
# MAGIC * **Result:** "2024-04-23" -> **4** (April)
# MAGIC
# MAGIC **3. Flag Weekend Operations (`is_weekend`)**
# MAGIC * **Scenario:** The Operations team needs to track shipments handled during weekends to calculate overtime pay or analyze non-business day capacity.
# MAGIC * **Action:** Flag as **'True'** if the `shipment_date` falls on a Saturday or Sunday.
# MAGIC
# MAGIC **4. Flag shipment status (`is_expedited`)**
# MAGIC * **Scenario:** The Operations team needs to track shipments is IN_TRANSIT or DELIVERED.
# MAGIC * **Action:** Flag as **'True'** if the `shipment_status` IN_TRANSIT or DELIVERED.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Enrichment/Business Logics (Calculated Fields)
# MAGIC *Deriving new metrics and financial indicators using mathematical and date-based operations.*<br>
# MAGIC Source File: logistics_shipment_detail_3000.json<br>
# MAGIC
# MAGIC **1. Calculate Unit Cost (`cost_per_kg`)**
# MAGIC * **Scenario:** The Finance team wants to analyze the efficiency of shipments by determining the cost incurred per unit of weight.
# MAGIC * **Action:** Divide `shipment_cost` by `shipment_weight_kg`.
# MAGIC * **Logic:** `shipment_cost / shipment_weight_kg`
# MAGIC
# MAGIC **2. Track Shipment Age (`days_since_shipment`)**
# MAGIC * **Scenario:** The Operations team needs to monitor how long it has been since a shipment was dispatched to identify potential delays.
# MAGIC * **Action:** Calculate the difference in days between the `current_date` and the `shipment_date`.
# MAGIC * **Logic:** `datediff(current_date(), shipment_date)`
# MAGIC
# MAGIC **3. Compute Tax Liability (`tax_amount`)**
# MAGIC * **Scenario:** For invoicing and compliance, we must calculate the Goods and Services Tax (GST) applicable to each shipment.
# MAGIC * **Action:** Calculate 18% GST on the total `shipment_cost`.
# MAGIC * **Logic:** `shipment_cost * 0.18`

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Remove/Eliminate (drop, select, selectExpr)
# MAGIC *Excluding unnecessary or redundant columns to optimize storage and privacy.*<br>
# MAGIC Source File: DF of logistics_source1 and logistics_source2<br>
# MAGIC
# MAGIC **1. Remove Redundant Name Columns**
# MAGIC * **Scenario:** Since we have already created the `full_name` column in the Enrichment step, the individual name columns are now redundant and clutter the dataset.
# MAGIC * **Action:** Drop the `first_name` and `last_name` columns.
# MAGIC * **Logic:** `df.drop("first_name", "last_name")`

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Splitting & Merging/Melting of Columns
# MAGIC *Reshaping columns to extract hidden values or combine fields for better analysis.*<br>
# MAGIC Source File: DF of logistics_shipment_detail_3000.json<br>
# MAGIC **1. Splitting (Extraction)**
# MAGIC *Breaking one column into multiple to isolate key information.*
# MAGIC * **Split Order Code:**
# MAGIC   * **Action:** Split `order_id` ("ORD100000") into two new columns:
# MAGIC     * `order_prefix` ("ORD")
# MAGIC     * `order_sequence` ("100000")
# MAGIC * **Split Date:**
# MAGIC   * **Action:** Split `shipment_date` into three separate columns for partitioning:
# MAGIC     * `ship_year` (2024)
# MAGIC     * `ship_month` (4)
# MAGIC     * `ship_day` (23)
# MAGIC
# MAGIC **2. Merging (Concatenation)**
# MAGIC *Combining multiple columns into a single unique identifier or description.*
# MAGIC * **Create Route ID:**
# MAGIC   * **Action:** Merge `source_city` ("Chennai") and `destination_city` ("Pune") to create a descriptive route key:
# MAGIC     * `route_lane` ("Chennai->Pune")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Customization & Processing - Application of Tailored Business Specific Rules
# MAGIC
# MAGIC ### **UDF1: Complex Incentive Calculation**
# MAGIC **Scenario:** The Logistics Head wants to calculate a "Performance Bonus" for drivers based on tenure and role complexity.
# MAGIC
# MAGIC **Action:** Create a Python function `calculate_bonus(role, age)` and register it as a Spark UDF.
# MAGIC
# MAGIC **Logic:**
# MAGIC * **IF** `Role` == 'Driver' **AND** `Age` > 50:
# MAGIC   * `Bonus` = 15% of Salary (Reward for Seniority)
# MAGIC * **IF** `Role` == 'Driver' **AND** `Age` < 30:
# MAGIC   * `Bonus` = 5% of Salary (Encouragement for Juniors)
# MAGIC * **ELSE**:
# MAGIC   * `Bonus` = 0
# MAGIC
# MAGIC **Result:** A new derived column `projected_bonus` is generated for every row in the dataset.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **UDF2: PII Masking (Privacy Compliance)**
# MAGIC **Scenario:** For the analytics dashboard, we must hide the full identity of the staff to comply with privacy laws (GDPR/DPDP), while keeping names recognizable for internal managers.
# MAGIC
# MAGIC **Business Rule:** Show the first 2 letters, mask the middle characters with `****`, and show the last letter.
# MAGIC
# MAGIC **Action:** Create a UDF `mask_identity(name)`.
# MAGIC
# MAGIC **Example:**
# MAGIC * **Input:** `"Rajesh"`
# MAGIC * **Output:** `"Ra****h"`
# MAGIC <br>
# MAGIC **Note: Convert the above udf logic to inbult function based transformation to ensure the performance is improved.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Core Curation & Processing (Pre-Wrangling)
# MAGIC *Applying business logic to focus, filter, and summarize data before final analysis.*
# MAGIC
# MAGIC **1. Select (Projection)**<br>
# MAGIC Source Files: DF of logistics_source1 and logistics_source2<br>
# MAGIC * **Scenario:** The Driver App team only needs location data, not sensitive HR info.
# MAGIC * **Action:** Select only `first_name`, `role`, and `hub_location`.
# MAGIC
# MAGIC **2. Filter (Selection)**<br>
# MAGIC Source File: DF of json<br>
# MAGIC * **Scenario:** We need a report on active operational problems.
# MAGIC * **Action:** Filter rows where `shipment_status` is **'DELAYED'** or **'RETURNED'**.
# MAGIC * **Scenario:** Insurance audit for senior staff.
# MAGIC * **Action:** Filter rows where `age > 50`.
# MAGIC
# MAGIC **3. Derive Flags & Columns (Business Logic)**<br>
# MAGIC Source File: DF of json<br>
# MAGIC * **Scenario:** Identify high-value shipments for security tracking.
# MAGIC * **Action:** Create flag `is_high_value` = **True** if `shipment_cost > 50,000`.
# MAGIC * **Scenario:** Flag weekend operations for overtime calculation.
# MAGIC * **Action:** Create flag `is_weekend` = **True** if day is Saturday or Sunday.
# MAGIC
# MAGIC **4. Format (Standardization)**<br>
# MAGIC Source File: DF of json<br>
# MAGIC * **Scenario:** Finance requires readable currency formats.
# MAGIC * **Action:** Format `shipment_cost` to string like **"₹30,695.80"**.
# MAGIC * **Scenario:** Standardize city names for reporting.
# MAGIC * **Action:** Format `source_city` to Uppercase (e.g., "chennai" → **"CHENNAI"**).
# MAGIC
# MAGIC **5. Group & Aggregate (Summarization)**<br>
# MAGIC Source Files: DF of logistics_source1 and logistics_source2<br>
# MAGIC * **Scenario:** Regional staffing analysis.
# MAGIC * **Action:** Group by `hub_location` and **Count** the number of staff.
# MAGIC * **Scenario:** Fleet capacity analysis.
# MAGIC * **Action:** Group by `vehicle_type` and **Sum** the `shipment_weight_kg`.
# MAGIC
# MAGIC **6. Sorting (Ordering)**<br>
# MAGIC Source File: DF of json<br>
# MAGIC * **Scenario:** Prioritize the most expensive shipments.
# MAGIC * **Action:** Sort by `shipment_cost` in **Descending** order.
# MAGIC * **Scenario:** Organize daily dispatch schedule.
# MAGIC * **Action:** Sort by `shipment_date` (Ascending).
# MAGIC
# MAGIC **7. Limit (Top-N Analysis)**<br>
# MAGIC Source File: DF of json<br>
# MAGIC * **Scenario:** Dashboard snapshot of critical delays.
# MAGIC * **Action:** Filter for 'DELAYED', Sort by Cost, and **Limit to top 10** rows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Wrangling - Transformation & Analytics
# MAGIC *Combining, modeling, and analyzing data to answer complex business questions.*
# MAGIC
# MAGIC ### **1. Joins**
# MAGIC Source Files:<br>
# MAGIC Left Side (staff_df):<br> DF of logistics_source1 & logistics_source2<br>
# MAGIC Right Side (shipments_df):<br> DF of logistics_shipment_detail_3000.json<br>
# MAGIC #### **1.1 Frequently Used Simple Joins (Inner, Left)**
# MAGIC * **Inner Join (Performance Analysis):**
# MAGIC   * **Scenario:** We only want to analyze *completed work*. Connect Staff to the Shipments they handled.
# MAGIC   * **Action:** Join `staff_df` and `shipments_df` on `shipment_id`.
# MAGIC   * **Result:** Returns only rows where a staff member is assigned to a valid shipment.
# MAGIC * **Left Join (Idle Resource check):**
# MAGIC   * **Scenario:** Find out which staff members are currently *idle* (not assigned to any shipment).
# MAGIC   * **Action:** Join `staff_df` (Left) with `shipments_df` (Right) on `shipment_id`. Filter where `shipments_df.shipment_id` is NULL.
# MAGIC
# MAGIC #### **1.2 Infrequent Simple Joins (Self, Right, Full, Cartesian)**
# MAGIC * **Self Join (Peer Finding):**
# MAGIC   * **Scenario:** Find all pairs of employees working in the same `hub_location`.
# MAGIC   * **Action:** Join `staff_df` to itself on `hub_location`, filtering where `staff_id_A != staff_id_B`.
# MAGIC * **Right Join (Orphan Data Check):**
# MAGIC   * **Scenario:** Identify shipments in the system that have *no valid driver* assigned (Data Integrity Issue).
# MAGIC   * **Action:** Join `staff_df` (Left) with `shipments_df` (Right). Focus on NULLs on the left side.
# MAGIC * **Full Outer Join (Reconciliation):**
# MAGIC   * **Scenario:** A complete audit to find *both* idle drivers AND unassigned shipments in one view.
# MAGIC   * **Action:** Perform a Full Outer Join on `shipment_id`.
# MAGIC * **Cartesian/Cross Join (Capacity Planning):**
# MAGIC   * **Scenario:** Generate a schedule of *every possible* driver assignment to *every* pending shipment to run an optimization algorithm.
# MAGIC   * **Action:** Cross Join `drivers_df` and `pending_shipments_df`.
# MAGIC
# MAGIC #### **1.3 Advanced Joins (Semi and Anti)**
# MAGIC * **Left Semi Join (Existence Check):**
# MAGIC   * **Scenario:** "Show me the details of Drivers who have *at least one* shipment." (Standard filtering).
# MAGIC   * **Action:** `staff_df.join(shipments_df, "shipment_id", "left_semi")`.
# MAGIC   * **Benefit:** Performance optimization; it stops scanning the right table once a match is found.
# MAGIC * **Left Anti Join (Negation Check):**
# MAGIC   * **Scenario:** "Show me the details of Drivers who have *never* touched a shipment."
# MAGIC   * **Action:** `staff_df.join(shipments_df, "shipment_id", "left_anti")`.
# MAGIC
# MAGIC ### **2. Lookup**<br>
# MAGIC Source File: DF of logistics_source1 and logistics_source2 (merged into Staff DF) and Master_City_List.csv<br>
# MAGIC * **Scenario:** Validation. Check if the `hub_location` in the staff file exists in the dataframe of corporate `Master_City_List.csv`.
# MAGIC * **Action:** Compare values against this Master_City_List list.
# MAGIC
# MAGIC ### **3. Lookup & Enrichment**<br>
# MAGIC Source File: DF of logistics_source1 and logistics_source2 (merged into Staff DF) and Master_City_List.csv dataframe<br>
# MAGIC * **Scenario:** Geo-Tagging.
# MAGIC * **Action:** Lookup `hub_location` (eg. "Pune") in a Master Latitude/Longitude Master_City_List.csv dataframe and enrich our logistics_source (merged dataframe) by adding `lat` and `long` columns for map plotting.
# MAGIC
# MAGIC ### **4. Schema Modeling (Denormalization)**<br>
# MAGIC Source Files: DF of All 3 Files (logistics_source1, logistics_source2, logistics_shipment_detail_3000.json)<br>
# MAGIC * **Scenario:** Creating a "Gold Layer" Table for PowerBI/Tableau.
# MAGIC * **Action:** Flatten the Star Schema. Join `Staff`, `Shipments`, and `Vehicle_Master` into one wide table (`wide_shipment_history`) so analysts don't have to perform joins during reporting.
# MAGIC
# MAGIC ### **5. Windowing (Ranking & Trends)**<br>
# MAGIC Source Files:<br>
# MAGIC DF of logistics_source2: Provides hub_location (Partition Key).<br>
# MAGIC logistics_shipment_detail_3000.json: Provides shipment_cost (Ordering Key)<br>
# MAGIC * **Scenario:** "Who are the Top 3 Drivers by Cost in *each* Hub?"
# MAGIC * **Action:**
# MAGIC   1. Partition by `hub_location`.
# MAGIC   2. Order by `total_shipment_cost` Descending.
# MAGIC   3. Apply `dense_rank()` and `row_number()
# MAGIC   4. Filter where `rank or row_number <= 3`.
# MAGIC
# MAGIC ### **6. Analytical Functions (Lead/Lag)**<br>
# MAGIC Source File: <br>
# MAGIC DF of logistics_shipment_detail_3000.json<br>
# MAGIC * **Scenario:** Idle Time Analysis.
# MAGIC * **Action:** For each driver, calculate the days elapsed since their *previous* shipment.
# MAGIC
# MAGIC ### **7. Set Operations**<br>
# MAGIC Source Files: DF of logistics_source1 and logistics_source2<br>
# MAGIC * **Union:** Combining `Source1` (Legacy) and `Source2` (Modern) into one dataset (Already done in Active Munging).
# MAGIC * **Intersect:** Identifying Staff IDs that appear in *both* Source 1 and Source 2 (Duplicate/Migration Check).
# MAGIC * **Except (Difference):** Identifying Staff IDs present in Source 2 but *missing* from Source 1 (New Hires).
# MAGIC
# MAGIC ### **8. Grouping & Aggregations (Advanced)**<br>
# MAGIC Source Files:<br>
# MAGIC DF of logistics_source2: Provides hub_location and vehicle_type (Grouping Dimensions).<br>
# MAGIC DF of logistics_shipment_detail_3000.json: Provides shipment_cost (Aggregation Metric).<br>
# MAGIC * **Scenario:** The CFO wants a subtotal report at multiple levels:
# MAGIC   1. Total Cost by Hub.
# MAGIC   2. Total Cost by Hub AND Vehicle Type.
# MAGIC   3. Grand Total.
# MAGIC * **Action:** Use `cube("hub_location", "vehicle_type")` or `rollup()` to generate all these subtotals in a single query.

# COMMAND ----------

# MAGIC %md
# MAGIC ##6. Data Persistance (LOAD)-> Data Publishing & Consumption<br>
# MAGIC
# MAGIC Store the inner joined, lookup and enrichment, Schema Modeling, windowing, analytical functions, set operations, grouping and aggregation data into the delta tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ##7.Take the copy of the above notebook and try to write the equivalent SQL for which ever applicable.
