# Lab 02: Ingest data with a pipeline

### Estimated Duration: 120 Minutes

## Overview

In this lab, you will learn how to use Data Factory pipelines in Microsoft Fabric to implement data ingestion solutions. You will create a pipeline that uses a Copy Data activity to copy data from an external source into your lakehouse, and then use a Spark notebook to transform the ingested data and load it into a table. Finally, you will modify the pipeline to include the notebook as part of the workflow.

## Objectives

In this lab, you will complete the following tasks:

 - Task 1: Create a pipeline
 - Task 2: Create a notebook
 - Task 3: Modify the pipeline

## Architecture Diagram

![](./Images/lab2img.png)

## Use Data Factory pipelines in Microsoft Fabric

A data lakehouse is a common analytical data store for cloud-scale analytics solutions. One of the core tasks of a data engineer is to implement and manage the ingestion of data from multiple operational data sources into the lakehouse. In Microsoft Fabric, you can implement *extract, transform, and load* (ETL) or *extract, load, and transform* (ELT) solutions for data ingestion through the creation of *pipelines*.

Fabric also supports Apache Spark, enabling you to write and run code to process data at scale. By combining the pipeline and Spark capabilities in Fabric, you can implement complex data ingestion logic that copies data from external sources into the OneLake storage on which the lakehouse is based, and then uses Spark code to perform custom data transformations before loading it into tables for analysis.

## Task 1: Create a pipeline

In this task, you will create a pipeline that ingests data from an external source into your lakehouse. You will use a Copy Data activity to copy the data, and then use a Spark notebook to transform the ingested data and load it into a table.

1. Navigate back to the workspace **dp_fabric-<inject key="DeploymentID" enableCopy="false"/>**. Click on **+ New item (1)**, in the search box, search for **Pipeline (2)** and select **Pipeline (3)** from the list.

    ![](./Images/l2T1S1.png)

1. Create a new data pipeline named **Ingest Sales Data Pipeline (1)** make sure the location is set to **dp_fabric-<inject key="DeploymentID" enableCopy="false"/> (2)** and have the check box **(3)** enabled, then click on **Create (4)**. 
    
    ![](./Images/l2T1S2.png)
   
1. On the **Build a data pipeline to organize and move your data** page, select **Copy data assistant**.

   ![03](./Images/l2T1S3.png)

1. In the **Copy data** wizard, on the **Choose data source** page, search for **Http (1)** and select the **Http (2)** source from the results.

   ![](./Images/E1T4S5.png)

1. In the **Connection settings** pane, enter the following settings for the connection to your data source:
    
    - URL: Enter the URL Below **(1)**
        ```
        https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/sales.csv
        ```
    - Connection: **Create new connection (2)**
    - Connection name: **Connection<inject key="DeploymentID" enableCopy="false"/> (3)**
    - Authentication kind: **Anonymous (4)**
    - Leave everything else as default
    - Click on **Next (5)**
  
      ![03](./Images/E1T4S6.png)
    
1. On the **Choose data** pane, keep the default settings and click **Next**.
    
    ![05](./Images/l2T1S6.png)
   
1. Wait for the data to be sampled, then verify the following settings:

   - **File format:** DelimitedText **(1)**
   - **Column delimiter:** Comma (,) **(2)**
   - **Row delimiter:** Line feed (\n) **(3)**
   - Click **Preview data (4)** to view a sample of the data.
   - After reviewing, close the preview and click **Next (5)**.

        ![Account-manager-start](./Images/E1T4S8i.png)

1. On the **Choose data destination** page, click **OneLake catalog (1)** and select the lakehouse **fabric_lakehouse (2)**.
    
    ![](./Images/l2T1S9.png)

1. On the **Settings** page, select **Full copy (1)**, scroll down to set the destination root folder set to **Files (2)** and then click **Next (3)** to proceed.

    ![](./Images/E1T2S10.png)

1. On the **Map to destination** page, set **Folder path** to **new_data (1)** and **File name** to **sales.csv (1)**.
    
    ![08](./Images/E1T2S11.png)

1. Set the following file format options and leave all other settings at their default values:

   - File format: **DelimitedText (1)**
   - Click **File format Settings (2)**
   - Column delimiter: **Comma (,) (3)**
   - Row delimiter: **Line feed (\n) (4)**
   - Leave all other settings as default and Click **Next (5)**
   
        ![09](./Images/E1T4S12.png)

1. On the **Review + save** page, verify the source and destination details, then click **Save** to create and run the copy job.

    ![09](./Images/l2T1S12.png)

1. A new pipeline containing a **Copy data** activity is created, as shown here:

    ![](./Images/cpdta.png)

1. When the pipeline starts to run, you can monitor its status in the **Output** pane under the pipeline designer. Use the **&#8635;** (*Refresh*) icon to refresh the status, and wait until it has succeeded.

    > **Note:** If you don't see any Output status, click on **View run status** on the top menu or check the notifications for a successful output.

    ![](./Images/l2T1S14.png)

1. If you don’t see any run status in the **Output** pane, click **Run** on the top menu to manually start the pipeline.

    ![09](./Images/upfab-ric-ex1-g14.png)

1.  When prompted, click on **Save and run** to start the pipeline.

    ![09](./Images/fab-ric-ex1-g15.png)

    > **Note:** If any errors appear while running the pipeline, review the details in the notification panel, fix the issue, and run it again. If everything succeeds, you can skip below steps and proceed with **Step 18**.

    - If the **Connection** field shows an error, select the **Copy job (1)** and switch to  **Settings (2)**, click on the dropdown **(3)** and select **Browse all (4)** to choose the correct connection manually.

        ![09](./Images/manualcon.png)
    
    - From the **Get data** page, select **Copy job (1)** under the **New sources** section to continue.

        ![09](./Images/fab-ric-cor-g3.png)
    
    - Set the following connection details:

      - Connection name: **Connection<inject key="DeploymentID" enableCopy="false"/> (1)**
      - Click **Sign in (2)** to authenticate if it shows You are not signed in.

        ![09](./Images/upfab-ric-cor-g4.png)
    
    - When prompted to sign in, select your **ODL_User** account or sign in manually using:
       - **Email/Username:** <inject key="AzureAdUserEmail"></inject>
       - **Temporary Access Pass:** <inject key="AzureAdUserPassword"></inject>

        ![09](./Images/upfab-ric-cor-g5.png)
    
    - After the connection details are verified and you are signed in, click **Connect** to proceed.
    
    - Once the **Copy job (1)** is configured, click **Run (2)** at the top to execute the pipeline.

       ![09](./Images/upfab-ric-cor-g7.png)
    
    - When prompted, click on **Save and run** to start the pipeline.

      ![09](./Images/fab-ric-ex1-g15.png)

1. From the Top bar navigate to your Lakehouse by clicking on the **fabric_lakehouse (1)**, expand **Files (2)** and select the **new_data (3)** folder, refresh the page and verify that the **sales.csv (4)** file has been copied.

    ![Account-manager-start](./Images/new_data1.png)

    >**Note:** You can also navigate to your Lakehouse by clicking your workspace and selecting the Lakehouse.

## Task 2: Create a notebook

In this task, you will create a Spark notebook to transform the ingested data and load it into a table. You will then run the notebook to verify that the data is correctly transformed and loaded.

1. On the **Home** page for your lakehouse, click on the **3-dots (1)** and select the **Open notebook (2)** menu, then click on **New notebook (3)**.

   ![](./Images/L2T2S1-2302.png)

    After a few seconds, a new notebook containing a single *cell* will open. Notebooks are made up of one or more cells that can contain *code* or *markdown* (formatted text).

1. Select the existing cell in the notebook, which contains some simple code, and then replace the default code with the following variable declaration.

    ```python
   table_name = "sales"
    ```

1. Open the **ellipsis (1)** menu for the cell and select **Toggle parameter cell (2)** to mark this cell as a parameter cell for pipeline runs.

    ![](./Images/ns-fab-g3.png)

1. Below the parameters cell, select **+ Code (1)** to insert a new code cell, then paste the transformation code into that cell (2).

    ```python
   from pyspark.sql.functions import *

   # Read the new sales data
   df = spark.read.format("csv").option("header","true").load("Files/new_data/*.csv")

   ## Add month and year columns
   df = df.withColumn("Year", year(col("OrderDate"))).withColumn("Month", month(col("OrderDate")))

   # Derive FirstName and LastName columns
   df = df.withColumn("FirstName", split(col("CustomerName"), " ").getItem(0)).withColumn("LastName", split(col("CustomerName"), " ").getItem(1))

   # Filter and reorder columns
   df = df["SalesOrderNumber", "SalesOrderLineNumber", "OrderDate", "Year", "Month", "FirstName", "LastName", "EmailAddress", "Item", "Quantity", "UnitPrice", "TaxAmount"]

   # Load the data into a table
   df.write.format("delta").mode("append").saveAsTable(table_name)
    ```

    ![](./Images/fab-ms-ex1-g46.png)

    This code loads the data from the sales.csv file that was ingested by the **Copy Data** activity, applies some transformation logic, and saves the transformed data as a table - appending the data if the table already exists.

1. Verify that your notebooks look similar to this, and then use the **&#9655; Run all** button on the toolbar to run all of the cells it contains.

    ![](./Images/runall.png)

1. After the notebook run completes, open the ⚙️ **Settings (1)** panel and update the **Name (2)** of the notebook to **Load Sales**, then close the pane.

    ![](./Images/fab-ms-ex1-g50.png)

1. In the **Explorer** pane of your Lakehouse, from the eplipses menu **(1)** of Tables, click on **Refresh (2)**.  Then expand **Tables**, and select the **sales (3)** table to see a preview of the data it contains.

   ![](./Images/L2T2S7-2302.png)

## Task 3: Modify the pipeline

In this task, you will modify the pipeline you created in Task 1 to include the notebook you created in Task 2. This will allow you to run the notebook as part of the pipeline workflow, enabling you to automate the data transformation and loading process.

1. In the left navigation menu bar, select the **Ingest Sales Data** pipeline you created previously.

1. Open the **Activities (1)** tab, select the **More activities (2)** menu, and choose **Delete data (3)**. Drag the new Delete data activity to the left of the Copy data activity and connect its **On completion** output to Copy data.

    ![](./Images/fab-ms-ex1-g51.png)

    ![Screenshot of a pipeline with Delete data and Copy data activities.](./Images/L2T3S2.2-2302.png)

1. Select the **Delete data** activity and, in the pane below the canvas, set **General (1)** → **Name (2)** to **Delete old files**.

    ![](./Images/fab-ms-ex1-g52.png)

1. In the **Source** section, configure the following:
    - **Connection**: Click on the dropdown menu **(1)** and select **Browse all (2)**. On the Choose a data source to get started, select your **Lakehouse (3)**.  
    - **File path type (4)**: Wildcard file path  
    - **Folder path (5)**: Files/new_data
    - **Wildcard file name (6)**: *.csv  
    - **Recursively (7)**: Selected

        ![](./Images/L2T3S4.1-2302.png)

        ![](./Images/L2T3S4.2-2302.png)

        ![](./Images/L2T3S4.3-2302.png)                

1. In the **Logging settings**, ensure **Enable logging** is **unselected**.

    ![](./Images/fab-ms-ex1-g54.png)

    >**Note:** These settings will ensure that any existing .csv files are deleted before copying the **sales.csv** file.

1. In the pipeline designer, select **Notebook** to add a **Notebook** activity to the pipeline.

    ![](./Images/fab-ms-ex1-g55.png)

1. Select the **Copy data** activity and then connect its **On completion** output to the **Notebook** activity as shown here:

    ![](./Images/notebookpline1.png)

1. Select the **Notebook** activity, and then in the pane below the design canvas, set the following properties:
    - **General**:
        - **Name**: Load Sales notebook

            ![](./Images/lsn.png)
    
    - **Settings**:
        - **Notebook (1)**: Load Sales
        - **Base parameters (2)**: *Add a new parameter with the following properties:*
            
            | Name | Type | Value |
            | -- | -- | -- |
            | table_name | String | new_sales |

            ![](./Images/L2T3S8.2-2302.png)

    The **table_name** parameter will be passed to the notebook and override the default value assigned to the **table_name** variable in the parameters cell.

1. On the **Home (1)** tab, use the **&#128427;(2)** (*Save*) icon to save the pipeline. Then use the **&#9655; Run (3)** button to run the pipeline, and wait for all of the activities to complete.

    ![](./Images/L2T3S9-2302.png)

1. Navigate to your **Lakehouse** from the left navigation menu. 

    ![](./Images/l2T3S10.png)

1. In the **Explorer** pane, refresh and expand **Tables** and select the **new_sales** table to see a preview of the data it contains. This table was created by the notebook when it was run by the pipeline.

   ![](./Images/newsalesdata1.png)

## Summary

In this lab, you have gained hands-on experience with using Data Factory pipelines in Microsoft Fabric to implement data ingestion solutions. You created a pipeline that uses a Copy Data activity to copy data from an external source into your lakehouse, and then used a Spark notebook to transform the ingested data and load it into a table. Finally, you modified the pipeline to include the notebook as part of the workflow, enabling you to automate the data transformation and loading process.

### You have successfully completed the lab.

By completing the **Work with Delta Lake and Data Factory pipelines in Microsoft Fabric** hands-on lab, you have gained practical experience in working with **Microsoft Fabric Lakehouses** and **Data Factory Pipelines** to manage and process data efficiently. You learned how to store and manage data using **Delta Lake tables**, enabling reliable, scalable, and versioned data processing through Apache Spark.

Additionally, you designed and implemented automated **ETL workflows using Fabric pipelines** to ingest, transform, and load data into your lakehouse. You also explored how to use Spark notebooks for data transformation and how to integrate them into your pipelines for end-to-end automation.

Overall, you have developed essential skills in leveraging Microsoft Fabric's capabilities for building robust data engineering solutions, enabling you to efficiently manage and analyze data in a modern data architecture.

