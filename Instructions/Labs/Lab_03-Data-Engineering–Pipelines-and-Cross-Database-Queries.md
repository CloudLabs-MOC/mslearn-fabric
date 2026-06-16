# Lab 03: Data Engineering – Pipelines and Cross-Database Queries

### Estimated Duration: 60 Minutes

## 📘 Lab Scenario

Contoso Retail has established a Lakehouse and a Data Warehouse in Microsoft Fabric. Now, the data engineering team needs to automate the data movement between these assets. Instead of manually uploading files and running scripts, you will use **Data Pipelines** to ingest data into the Lakehouse, transform it into Delta tables, and load curated data from the Lakehouse into the Data Warehouse using **cross-database queries** all orchestrated end-to-end through pipelines.

As a Data Engineer, you will build a production-style data pipeline that mirrors real-world ETL patterns: ingest raw data, transform it using notebooks, and load refined data into the warehouse for reporting.

## 📋 Overview

Microsoft Fabric Data Pipelines provide a visual orchestration engine that enables organizations to move, transform, and manage data efficiently across various Fabric items. When combined with cross-database (cross-object) queries, data can be accessed directly from Lakehouse Delta tables through the Warehouse SQL endpoint, eliminating the need for redundant data copies and simplifying data integration. In this lab, you will learn how to create a Data Pipeline to ingest external data into a Lakehouse, use a Notebook activity to transform raw data into a curated Delta table, leverage cross-database queries to load data from the Lakehouse into a Data Warehouse, and orchestrate the entire workflow within a single pipeline using proper activity sequencing and dependencies.

## 🏗️ Architecture Diagram

   ![](<./Images/archlab03.png>)

## 🎯 Lab objectives

In this lab, you will complete the following tasks:

- Task 1: Create a Notebook for data transformation
- Task 2: Create a Data Pipeline
- Task 3: Add a Copy Activity to ingest data into the Lakehouse
- Task 4: Add a Notebook Activity to transform data
- Task 5: Use cross-database query to load data into the Warehouse
- Task 6: Run and monitor the pipeline

## Task 1: Create a Notebook for data transformation

In this task, you will create a Spark notebook that transforms the raw CSV data (ingested into the Lakehouse Files area) into a cleansed Delta table. This notebook will later be called from the pipeline.

1. In the Microsoft Fabric portal, make sure you are in workspace **Workspace-<inject key="DeploymentID" enableCopy="false"/>**, then click **Power BI** **(1)** on the left navigation bar, and click **+ New item** **(2)**.

   ![](<./Images/L2T1S1.png>)

1. On the **New item** page, search for **notebook** **(1)** in the search bar and select **Notebook** **(2)**.

   ![](<./Images/Img1.png>)

   > **Note**: If prompted to select a Lakehouse, choose **Lakehouse_<inject key="DeploymentID" enableCopy="false"/>** and click **Add**.

1. On the New Notebook pop-up rename it to **nb_transform_products** **(1)** and click on **Create (2)**.

    ![](<./Images/img2.png>)

1. If the notebook does not have a default Lakehouse attached, click **Add data items (1)** in the **Data Items** section of the left **Explorer** pane, and click on **From OneLake catalog (2)**
    
    ![](<./Images/img3.png>)

1. On the other window choose **Lakehouse_<inject key="DeploymentID" enableCopy="false"/>** **(1)**, and click **Add(2)**.
    
    ![](<./Images/img4.png>)

1. First, upload the sample data so you can test the notebook. In the **Explorer** pane on the left, click the ellipses **(...) (1)** next to the **Files** folder, then select **New subfolder (2)**. Name it **raw** and click **Create**.
   
    ![](<./Images/img5.png>)

    ![](<./Images/img111.png>)

1. Click the ellipses **(...) (1)** next to the **raw** folder, hover over **Upload (2)**, then select **Upload files (3)**. Download the file from [this link](https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/products.csv).

   ![](<./Images/img7.png>)
   ![](<./Images/img111.png>)
   >**Note:** To download the file, open a new tab in the browser and paste it into the URL.

   > Right-click anywhere on the page containing the data and select Save as to save the page as a CSV file.

1. On the Upload files dialog, click the folder icon on the right to browse, go to path **C:\LabFiles\dp-data-main** and select the **prodcuts.csv (1)** file from your local or lab machine and click on **Open (2)**

      ![](<./Images/img999.png>)

1. On the Upload files dialog, after selecting the **prodcuts.csv** file, click **Upload (1)** to upload the file into the raw folder.

    ![](<./Images/images11.png>)

1. Once the upload is complete and the status shows **Completed**, click the **Close** icon at the top right to exit the **Upload files** dialog.

      ![](<./Images/img1111.png>)

   - if you are using the lab virtual machine (lab VM) provided to you, you can get the file from the **C:\LabFiles\dp-data-main**  directory.

1. In the first cell, paste the following PySpark code and click the **&#9655; Run (1)** button to execute it:

   ```python
   # Read the raw CSV file from the Lakehouse Files area
   df_raw = spark.read.format("csv") \
       .option("header", "true") \
       .option("inferSchema", "true") \
       .load("Files/raw/products.csv")

   # Display the raw data
   print(f"Raw row count: {df_raw.count()}")
   df_raw.show(5)
   ```
    ![](<./Images/img8.png>)

   > **Note**: The code loads the products.csv file into a Spark DataFrame, displays the total number of rows in the dataset, and shows the first 5 records for validation. You should see **295 rows** and the columns **ProductID, ProductName, Category, and ListPrice.(2)**.

1. Click **+ Code** below the first cell to add a new code cell. Paste the following transformation code and **&#9655; Run** button:

    ![](<./Images/img10.png>)

    ![](<./Images/img9.png>)

   ```python
   from pyspark.sql import functions as F

   # Clean and transform the data
   df_clean = (
       df_raw
       .dropna(subset=["ProductName", "Category", "ListPrice"])
       .withColumn("ListPrice", F.col("ListPrice").cast("decimal(10,2)"))
       .withColumn("ProductName", F.trim(F.col("ProductName")))
       .withColumn("Category", F.trim(F.col("Category")))
       .withColumn("LoadTimestamp", F.current_timestamp())
   )

   # Write as a managed Delta table in the Lakehouse
   df_clean.write.format("delta") \
       .mode("overwrite") \
       .saveAsTable("stg_products")

   print(f"Staged row count: {df_clean.count()}")
   df_clean.show(5)
   ```

   >**Note:** This code cleans the raw dataset by removing null values, standardizing column formats, adding a load timestamp, and saving the transformed data as a **Delta table** named **stg_products** in the Lakehouse.

1. In the **Explorer** pane, Click it on the **(...) (1)** of the onelake and click on  the **Refresh all sources (2)**. Expand **Tables** under the **dbo** verify that the **stg_products** table now appears.

    ![](<./Images/img12.png>)

    ![](<./Images/img11.png>)

## Task 2: Create a Data Pipeline

In this task, you will create a new Data Pipeline that will orchestrate the entire data flow - from ingestion to transformation to loading.

1. In the hub menu bar on the left, click on your workspace **Workspace-<inject key="DeploymentID" enableCopy="false"/> (1)**.

1. Click **+ New item** **(1)** at the top of the workspace.

   ![](<./Images/E1T2S1.png>)

1. On the **New item** page, search for **Pipeline** **(1)** in the search bar and select **Pipeline** **(2)**.

    ![](<./Images/img13.png>)

1. In the **New pipeline** popup, enter **pl_ingest_and_load** **(1)** as the pipeline name, then click **Create** **(2)**.

   ![](<./Images/img14.png>)

1. The pipeline canvas opens with an empty design surface. You will add activities in the next tasks.

   ![](<./Images/img15.png>)

## Task 3: Add a Copy Activity to ingest data into the Lakehouse

In this task, you will add a **Copy Data** activity to the pipeline. This activity will download a sample products CSV file from an external HTTP source and land it in the Lakehouse **Files** folder.

1. On the pipeline canvas, click **Activity (1)** and click on **Copy data** **(2)** from the **Activities** toolbar and select **Add copy data activity (3)**.

   ![](<./Images/img16.png>)

1. Select the **Copy data (1)** activity on the canvas and click the **General** tab at the bottom. Set the **Name** to **Products CSV** **(2)**.

   ![](<./Images/img17.png>)

1. Click the **Source** **(1)** tab. Under **Fabric item connections**, click **Browse all** **(2)** to create a new connection.

   ![](<./Images/img18.png>)

1. In the **Choose a data source to get started** dialog:

   - Search **http** **(1)** as the data source type and select **HTTP (2)**
    
        ![](<./Images/img19.png>)

   - Under the connection settings in the **URL field** **(1)**, enter the following and click on **Connect (2)**:

     ```
     https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/products.csv
     ```
     
     ![](<./Images/img20.png>)

   - Set **Authentication kind** to **Anonymous** **(3)**.
   - Click **Create** **(4)**.

        ![](<./Images/img22.png>)

1. Back on the **Source** tab, under **File format**, confirm the format is set to **DelimitedText**.

   ![](<./Images/img21.png>)
  
1. Click the **Destination** tab:

   - Under **Connection** dropdown click on **Browse all**.
   - On the next screen scroll down to **OneLake Catalog**, select **Lakehouse**.
   - Select **Lakehouse_<inject key="DeploymentID" enableCopy="false"/>** as the Lakehouse.
        
        ![](<./Images/img23.png>)
    
   - Set **Root folder** to **Files (1)**.
   - In the **File path** field, enter the folder path **raw** **(2)** and file name **products.csv** **(2)**.

        ![](<./Images/img24.png>)

        > **Note**: The file will be stored at Files/raw/products.csv in the Lakehouse, which is the location expected by the notebook. If you manually uploaded the file in Task 1, the Copy activity will replace the existing file each time the pipeline runs. This is expected behavior and ensures that the pipeline always uses the latest copy of the source file without requiring manual uploads.

## Task 4: Add a Notebook Activity to transform data

In this task, you will add a **Notebook** activity after the Copy activity. This will execute the transformation notebook you created in Task 1 to convert the raw CSV into a Delta table.

1. On the pipeline canvas, from the **Activities** toolbar, select **Notebook** **(1)** to add a Notebook activity.

   ![](<./Images/img26.png>)

1. Select the **Notebook (1)** activity on the canvas and click the **General (2)** tab. Set the **Name** to **Transform to Delta** **(3)**.

   ![](<./Images/img27.png>)

1. Click the **Settings** **(1)** tab:

   - Under **Notebook**, click the dropdown and select **nb_transform_products** **(2)**.

        ![](<./Images/img28.png>)

1. Now connect the two activities. Hover over the right edge of the **Copy Products CSV** activity until the green checkmark (&#10004;) connector appears. Drag it to the **Transform to Delta** activity.

   ![](<./Images/img29.png>)

   > **Note**: The green connector means the Notebook activity will only run **on success** of the Copy activity. This ensures transformation only occurs after data is successfully ingested.

## Task 5: Use cross-database query to load data into the Warehouse

In this task, you will add a **Script** activity that uses a cross-database query to read the Delta table from the Lakehouse and insert it into a table in the Data Warehouse - all without moving files.

1. First, you need to create the target table in the Warehouse. Open a **new browser tab** and navigate to your workspace **Workspace-<inject key="DeploymentID" enableCopy="false"/>**.

1. Select the **myDataWarehouse** **(1)** warehouse to open it.

   ![](<./Images/L2T4S1.png>)

1. On the **Home** tab, click the dropdown next to **New SQL query** **(1)**, then select **New SQL query** **(2)**.

   ![](<./Images/L1T62.png>)

1. Paste the following SQL and click **Run** to create the target table:

   ```sql
   CREATE TABLE dbo.DimProductStaging
   (
       ProductID INT,
       ProductName VARCHAR(100),
       Category VARCHAR(50),
       ListPrice DECIMAL(10,2),
       LoadTimestamp DATETIME2(4)
   );
   GO
   ```

   ![](<./Images/img30.png>)

1. Verify that the table **DimProductStaging** appears in the **Explorer** pane under **dbo > Tables**. Click **Refresh** if needed.

    ![](<./Images/img31.png>)

1. Now, test the **cross-database query**. In a new SQL query, run the following to verify you can read Lakehouse data from the Warehouse:

   ```sql
   SELECT TOP 10 *
   FROM Lakehouse_.dbo.stg_products;
   ```
   >**Note:** Please replace the Lakehouse_ with the actual value of your Lakehouse.

   > **Use:** Lakehouse_<inject key="DeploymentID" enableCopy="false"/>.dbo.stg_products

   ![](<./Images/img32.png>)

   > **Note**: Cross-database queries use the **three-part naming** convention: `LakehouseName.SchemaName.TableName`. Both the Lakehouse and Warehouse must be in the **same workspace** for this to work.

1. Switch back to the browser tab with your **pl_ingest_and_load** pipeline.

1. From the **Activities** toolbar, select **Script** **(1)** to add a Script activity to the pipeline canvas.

   ![](<./Images/img33.png>)

1. Select the **Script (1)** activity on the canvas and click the **General** tab. Set the **Name** to **Load to Warehouse** **(2)**.

   ![](<./Images/img34.png>)

1. Click the **Settings** **(1)** tab:

   - Under **Connection**, select your **myDataWarehouse** **(2)** warehouse connection.
   - Set **Script type** to **NonQuery** **(3)**.
   - In the **Script** field, paste the following SQL **(4)**:

   ```sql
   -- Truncate and reload the staging table using cross-database query
   TRUNCATE TABLE dbo.DimProductStaging;

   INSERT INTO dbo.DimProductStaging (ProductID, ProductName, Category, ListPrice, LoadTimestamp)
   SELECT
       ProductID,
       ProductName,
       Category,
       ListPrice,
       LoadTimestamp
   FROM Lakehouse_.dbo.stg_products;
   ```
   
    >**Note:** Please replace the Lakehouse_ with the actual value of your Lakehouse. 
    
    >Example: **Lakehouse_<inject key="DeploymentID" enableCopy="false"/>.dbo.stg_products**

   ![](<./Images/img35.png>)

1. Connect the **Transform to Delta** activity to the **Load to Warehouse** activity using the green (on success) connector, just as you did in Task 4.

   ![](<./Images/img36.png>)

1. Your completed pipeline should now look like this, with three activities connected in sequence:

   > Products CSV ──▶ Transform to Delta ──▶ Load to Warehouse

   ![](<./Images/img37.png>)

## Task 6: Run and monitor the pipeline

In this task, you will validate, run, and monitor the pipeline to ensure all three activities complete successfully.

1. Switch to **Home (1)** On the pipeline toolbar, click on **Validate** **(2)**. You can check for configuration errors or validation errors on the **Pipeline Validation Pane (3)**.

   ![](<./Images/img38.png>)

1. Once validation passes, click **Run** to execute the pipeline.

   ![](<./Images/img39.png>)

1. If prompted to save, click **Save and run**.

    ![](<./Images/img40.png>)

1. The **Output** tab opens at the bottom, showing the pipeline run progress. Monitor each activity status:

   - **Copy Products CSV** - should show **Succeeded** (&#10004;)
   - **Transform to Delta** - should show **Succeeded** (&#10004;)
   - **Load to Warehouse** - should show **Succeeded** (&#10004;)

        ![](<./Images/img41.png>)

   > **Note**: The pipeline run typically completes in 2-4 minutes. If an activity fails, click on it to see the error details and troubleshoot.

1. To verify the data landed in the Warehouse, switch to the browser tab with **myDataWarehouse**.

1. Open a **New SQL query** and **Run(2)** the following **Query(1)**. Also you can validate the **Result(3)**:

   ```sql
   SELECT COUNT(*) AS TotalProducts FROM dbo.DimProductStaging;
   GO
   ```
    ![](<./Images/img42.png>)

    >**Note:** You should see the Total products number that were ingested from the CSV, transformed in the notebook, and loaded via the cross-database query.

   ```sql
   SELECT TOP 10 * FROM dbo.DimProductStaging ORDER BY Category, ProductName;
   GO
   ```

   ![](<./Images/img43.png>)

   >**Note:** You should see the **top 10 product rows** that were ingested from the CSV, transformed in the notebook, and loaded via the cross-database query.

1. **(Optional)** To schedule the pipeline for recurring runs:

   - Go back to the pipeline **pl_ingest_and_load**.

      ![](<./Images/imgpipe.png>)

   - Click **Schedule** on the toolbar.

        ![](<./Images/img44.png>)
   
   - On the next screen, click on **+ Add schedule**.

        ![](<./Images/img45.png>)

   - Set the **Repeat** to **Daily** and choose a **Start date & time and End date & time.**

   - Click **Save**.

        ![](<./Images/img46.png>)

## Summary

In this exercise, you have accomplished the following:

- Created a Spark Notebook to transform raw CSV data into a managed Delta table in the Lakehouse
- Built a Data Pipeline with three orchestrated activities: Copy, Notebook, and Script
- Used a Copy Data activity to ingest external data into the Lakehouse Files area
- Used a Notebook activity to run Spark transformations within the pipeline
- Leveraged cross-database queries (three-part naming) to load data from the Lakehouse into the Data Warehouse without data duplication
- Executed and monitored the end-to-end pipeline run
- Learned how to schedule the pipeline for recurring automated runs
## Conclusion 
 In this lab, you explored the core data analytics and engineering capabilities of Microsoft Fabric. You learned how to create and work with Lakehouses and Data Warehouses, ingest and organize data from multiple sources, query and analyze data using SQL, and build interactive reports for business insights. You also gained experience in automating data movement and transformation processes using Data Pipelines and Notebooks, enabling end-to-end data workflows. Through these exercises, you developed practical skills in managing, transforming, analyzing, and visualizing data within Microsoft Fabric's unified analytics platform.

### Congratulations! You have successfully completed the Hands-on lab. Click on Next >>.
