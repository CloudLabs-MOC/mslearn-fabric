# Lab 1: Get Started with Real-Time Analytics in Microsoft Fabric

### Estimated Duration: 120 Minutes

Microsoft Fabric provides an end-to-end platform for data solutions, including real-time data analytics. Synapse Real-Time Analytics in Fabric uses a KQL Database to provide table storage and Kusto Query Language (KQL), which is a powerful tool for analyzing data. This structure provides an efficient way to find insights and patterns from textual or structured data. Moreover, KQL is optimized for data that includes a time series component, such as real-time data from log files or streaming services. With Real-Time Analytics, you can focus and scale up your analytics solution while democratizing data for the needs of your entire data organization.

## Lab Objectives

You will be able to complete the following tasks:

- Task 1: Create a workspace
- Task 2: Download file for KQL database and create a KQL database
- Task 3: Use KQL to query the sales table
- Task 4: Create a Power BI report from a KQL Query set

## Task 1: Create a workspace

This task will guide you through creating a workspace in Microsoft Fabric.

1. In the menu bar on the left, select **Workspaces (1)** (the icon looks similar to &#128455;) and click on **+ New workspace (2)**.

   ![](./Images/E1T1S1.png)

1. Create a new workspace named **dp_fabric-<inject key="Deployment ID" enableCopy="false"/> (1)**. Expand **Advanced (2)**, then under **License mode**, select **Trial (3)** and click **Apply (4)** to create and open the workspace.
   
   ![](./Images/E1T2S2-1.png)

   ![](./Images/E1T2S2-2.png)

   >**Note:** If you see a pop-up saying **"Upgrade to a paid Power BI license"**, click on **"Try free"** to proceed with the trial.

   ![](./Images/10062025(2).png)

1. When your new workspace opens, it should be empty, as shown here:

   ![Screenshot of an empty workspace in Power BI.](./Images/E1T2S3.png)
 
## Task 2: Download file for KQL database and create a KQL database

In this task, you will learn how to download a file for a KQL database and create a KQL database in Microsoft Fabric for querying and analyzing data efficiently.

Now that you have a workspace, download the data file for analysis. Using *Kusto Query Language (KQL)*, you can query static or streaming data in a table within a KQL database. To analyze sales data, create a table in the database and ingest the downloaded file.

1. Inside the provided LabVM, locate the **sales.csv** file for this exercise by navigating to **C:\LabFiles\dp-data-main**.

   >**Note:** Alternatively, if you are using your own machine, you can download and save the file from **[Sales.csv](https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/sales.csv)** and save it .

1. Return to the browser window with the **Microsoft Fabric** experience.

1. Navigate to **dp_fabric-<inject key="Deployment ID" enableCopy="false"/>** workspace from the hub menu bar on the left.

    ![](./Images/E1T3S3.png)

1. In the **dp_fabric-<inject key="Deployment ID" enableCopy="false"/>** page, click on **+ New item (1)** and in the search bar serch for **Eventhouse (2)** and select **Eventhouse (3)**.
   
   ![](./Images/E1T3S4.png)

1. Enter **Eventhouse-<inject key="Deployment ID" enableCopy="false"/> (1)** as the name and click **Create (2)**.

    ![](./Images/E1T3S5-1.png)

1. When the **Welcome to Eventhouse** pop-up appears, select **Get started**.

    ![](./Images/E1T3S5-2.png)

1. When the new eventhouse has been created, right-click on it **(1)**, select the option **Get data (2)**, then **Local file (3)**. 

    ![Screenshot of selected Fabric Experience home with RTA selected](./Images/E1T3S6.png)

1. Then use the wizard to import the data into a new table by selecting the following options: 

    - **Select or create a destination table:**
        - **Database:** *The database you created is already selected*
        - **Table:** *Create a new table named* **sales** by clicking on the **+** **sign** to the left of ***New table***

          ![New table wizard step one](./Images/E1T3S7-1.png)

        - You will now see the **Drag files here or Browse for files** hyperlink appear in the same window.

          ![New table wizard step two](./Images/E1T3S7-2.png)

        - Browse or drag your **sales.csv** onto the screen and wait for the Status box to change to a green check box, and then select **Next**

          ![New table wizard step three](./Images/E1T3S7-3.png)

        - In the screen below, you'll notice that the column headings are in the first row. Although the system has detected them, we still need to move the **First row is column header (1)** slider above these lines to prevent any errors. Once you adjust the slider, everything should appear correctly. Finally, click the **Finish (2)** button in the bottom right corner of the panel to proceed.

          ![New table wizard step five](./Images/E1T3S7-4.png)

        - Wait for the steps in the summary screen to complete, which include **(1):**
            - Create table (sales)
            - Create mapping (sales_mapping)
            - Data queuing
            - Blob ingestion
        - Select the **Close (2)** button

          ![New table wizard step six](./Images/E1T3S7-5.png)

> **Note:** In this task, you imported a very small amount of static data from a file, which is fine for the purposes of this task. In reality, you can use Kusto to analyze much larger volumes of data, including real-time data from a streaming source such as Azure Event Hubs.

## Task 3: Use KQL to query the sales table

This task will walk you through using KQL (Kusto Query Language) to query the Sales table and analyze data efficiently in Microsoft Fabric.

Now that you have a table of data in your database, you can use KQL code to query it.

1. Right-click on the **sales (1)** table, select the **Query with code (2)**, and then **Show any 100 records**.

   ![](./Images/E1T4S1.png)

1. A new pane will open with the query and its result. 

1. Modify the query as follows:

    ```kusto
   sales
   | where Item == 'Road-250 Black, 48'
    ```

1. Run the query. Then review the results, which should contain only the rows for sales orders for the *Road-250 Black, 48* product.

1. Modify the query as follows:

    ```kusto
   sales
   | where Item == 'Road-250 Black, 48'
   | where datetime_part('year', OrderDate) > 2020
    ```

1. Run the query and review the results, which should contain only sales orders for *Road-250 Black, 48* made after 2020.

1. Modify the query as follows:

    ```kusto
   sales
   | where OrderDate between (datetime(2020-01-01 00:00:00) .. datetime(2020-12-31 23:59:59))
   | summarize TotalNetRevenue = sum(UnitPrice) by Item
   | sort by Item asc
    ```
1. Run the query and review the results, which should contain the total net revenue for each product between January 1st and December 31st 2020, in ascending order of product name.

1. Navigate to **dp_fabric-<inject key="Deployment ID" enableCopy="false"/> (1)** workspace from the hub menu bar on the left. Select **Eventhouse-<inject key="Deployment ID" enableCopy="false"/> (2)** KQL Database from the list.

    ![](./Images/E1T4S9.png)

1. Select the **KQL queryset (1)**, click on **Edit (2)**.

   ![](./Images/E1T4S10-1.png)

11. Change the name of the KQL queryset to **Revenue by Product**.

    ![](./Images/E1T4S10-2.png)

## Task 4: Create a Power BI report from a KQL Query set

This task will guide you through visualizing and analyzing data effectively by creating a Power BI report using a KQL query set. You will learn how to transform query results into insightful reports and dashboards.

You can use your KQL Queryset as the basis for a Power BI report.

1. On the **Revenue by Product** KQL Queryset select **More (1)** then **Create Power BI report (2)**. Wait for the report editor to open.

    ![](./Images/E1T5S2.png)

1. In the report editor, in the **Data** pane, expand **Kusto Query Result (1)** and select the **Item** and **TotalNetRevenue** **(2)** fields.

    ![](./Images/E1T5S3.png)

1. On the report design canvas, select the table visualization that has been added and then in the **Visualizations** pane, select **Clustered bar chart**.

    ![Screenshot of a report from a KQL query.](./Images/E1T5S4.png)

1. In the **Power BI** window, in the **File (1)** menu, select **Save (2)**. 

   ![](./Images/E1T5S5.png)

1. Save the report as **Revenue by Item (1)** in the workspace **dp_fabric-<inject key="Deployment ID" enableCopy="false"/> (2)**. Then select **Continue (3)**.
  
    ![](./Images/E1T5S6.png)
    
1. Close the **Power BI** window, and in the bar on the left, select the icon for your workspace.

     ![](./Images/E1T5S7.png)

    Refresh the Workspace page if necessary to view all of the items it contains.

1. In the list of items in your workspace, note that the **Revenue by Item** report is listed.
   
## Summary:

In this lab, you have the opportunity to explore Microsoft Fabric as a platform for real-time analytics using Synapse Real-Time Analytics and Kusto Query Language (KQL). You will learn to assign the Fabric Administrator role, create a workspace, set up a KQL database, query data, and generate a Power BI report from KQL queries. This lab demonstrates how to efficiently analyze real-time data using KQL, particularly for time-series data like logs and streaming services.

You have completed the following tasks:

- Created a workspace
- Downloaded file for KQL database and created a KQL database
- Used KQL to query the sales table
- Created a Power BI report from a KQL Query set

## References:

- [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)
- [Kusto Query Language (KQL) Overview](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/)
- [Power BI Documentation](https://learn.microsoft.com/en-us/power-bi/)

Now, click on **Next >>** from the lower right corner to move on to the next page.

![](./Images/next.png)
