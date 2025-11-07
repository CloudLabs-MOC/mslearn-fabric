# Lab 02: Analyze data in a Data Warehouse

### Estimated Duration: 120 Minutes

## Overview

Data warehouses are analytical stores built on a relational schema to support SQL queries. Microsoft Fabric enables you to create a relational data warehouse in your workspace and integrate it easily with other elements of your end-to-end analytics solution.

Microsoft Fabric's data warehouse is a modern version of the traditional data warehouse. It centralizes and organizes data from different departments, systems, and databases into a single, unified view for analysis and reporting purposes. Fabric's data warehouse provides full SQL semantics, including the ability to insert, update, and delete data in the tables. Fabric's data warehouse is unique because it's built on the Lakehouse, which is stored in Delta format and can be queried using SQL. It's designed for use by the whole data team, not
just data engineers.

Here, you'll learn about data warehouses in Fabric, create a data warehouse, load, query, and visualize data, and describe datasets.

## Architecture Diagram

![New lakehouse.](./Images/archlab2.png)

## Lab objectives

In this lab, you will complete the following tasks:

- Task 1: Create a Data Warehouse
- Task 2: Create tables and insert data
- Task 3: Define a Data Model
- Task 4: Query data Warehouse tables
- Task 5: Create a View, Visual Query and Visualizing data

## Task 1: Create a Data Warehouse

In this task, you will provision a new data warehouse within Microsoft Fabric. A data warehouse in Fabric offers a fully managed relational database designed for large-scale analytics. Unlike the default read-only SQL endpoint provided for lakehouse tables, a data warehouse supports complete SQL capabilities, including inserting, updating, and deleting data. This enables you to design and manage structured schemas, load data efficiently, and prepare it for advanced querying and reporting.

1. In the Power BI portal, in the workspace **Workspace-<inject key="DeploymentID" enableCopy="false"/>**, then click **Power BI** **(1)** on the left navigation bar, and click **+ New item** **(2)** to create a new workspace item.

   ![](<./Images/L2T1S1.png>)

1. On the **New item** page, search for **Warehouse** in the search bar **(1)**, then select the **Warehouse** tile **(2)** to create a new warehouse.

   ![](<./Images/L2T1S2.png>)

1. On the **New warehouse** pane, enter **myDataWarehouse** **(1)** in the Name field, then click **Create** **(2)** to provision the warehouse.

   ![](<./Images/L2T1S3.png>)

## Task 2: Create tables and insert data

A warehouse is a relational database in which you can define tables and other objects.

1. In your new warehouse, select the **Create tables** with **T-SQL** tile, and replace the default SQL code with the following CREATE TABLE statement:

   ![Screenshot of a new warehouse.](<./Images/L2T2S2.png>)

   ```sql
   CREATE TABLE dbo.DimProduct
   (
      ProductKey INTEGER NOT NULL,
      ProductAltKey VARCHAR(25) NULL,
      ProductName VARCHAR(50) NOT NULL,
      Category VARCHAR(50) NULL,
      ListPrice DECIMAL(5,2) NULL
   );
   GO
   ```

1. Use the **&#9655; Run** button to run the SQL script, which creates a new table named **DimProduct** in the **dbo** schema of the data warehouse.

   ![](<./Images/L2T2S2.png>)

1. To refresh the Explorer pane and view the latest changes, click the **Refresh** icon in the top left menu bar.

   ![](<./Images/L2T2S3.png>)

1. In the **Explorer** pane, expand **myDataWarehouse** **(1)**, then expand the **Schemas** section **(2)** to view the available schema objects, including `dbo`, `INFORMATION_SCHEMA`, `queryinsights`, and `sys`.

   ![](<./Images/L2T2S4.png>)

1. On the **Home** tab, click the dropdown next to **New SQL query** **(1)**, then select **New SQL query** **(2)** to open a new query editor

   ![](<./Images/L1T62.png>)

1. After inserting the query, click **Run** to execute it. This query will insert three rows into the **DimProduct** table.

   ```sql
   INSERT INTO dbo.DimProduct
   VALUES
   (1, 'RING1', 'Bicycle bell', 'Accessories', 5.99),
   (2, 'BRITE1', 'Front light', 'Accessories', 15.49),
   (3, 'BRITE2', 'Rear light', 'Accessories', 15.49);
   GO
   ```

   ![](<./Images/L2T2S6.png>)

1. In the **Explorer** pane, expand **myDataWarehouse** **(1)**, then expand **Schemas** **(2)**, **dbo** **(3)**, and **Tables** **(4)**, then select the **DimProduct** table **(5)** and verify that the three inserted rows are displayed in the data preview.

   ![](<./Images/L2T2S7.png>)

1. On the **Home** tab, click the dropdown next to **New SQL query** **(1)**, then select **New SQL query** **(2)** to open a new query editor

   ![](<./Images/L1T62.png>)

1. On the **Home** menu tab, use the **New SQL Query** button to create a new query. Then copy and paste the Transact-SQL code from [https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/create-dw.txt](https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/create-dw.txt) into the new query pane.

1. Run the query, which creates a simple data warehouse schema and loads some data. The script should take around 30 seconds to run.

   ![](<./Images/cor_e2_fz_2.png>)

1. Use the **Refresh (1)** button on the toolbar to refresh the view. Then in the **Explorer** pane, verify that the **dbo** schema in the data warehouse now contains the following four **tables (2)**:

   - **DimCustomer**
   - **DimDate**
   - **DimProduct**
   - **FactSalesOrder**

      ![](<./Images/L2T2S11.png>)

      > **Tip**: If the schema takes a while to load, just refresh the browser page.

## Task 3: Define a Data Model

A relational data warehouse typically consists of fact and dimension tables. The fact tables contain numeric measures you can aggregate to analyze business performance (for example, sales revenue), and the dimension tables contain attributes of the entities by which you can aggregate the data (for example, product, customer, or time). In a Microsoft Fabric data warehouse, you can use these keys to define a data model that encapsulates the relationships between the tables.

1. Under the **Home** tab, Click on  **New semantic model** to create the semantic model.

   ![](<./Images/L2T3S1.png>)

1. Enter **dw semantic model** **(1)** in the name field and select the tables **DimCustomer**,**DimDate**,**DimProduct**,**FactSalesOrder** **(2)**, than click on **Confirm** **(3)** to proceed.

   ![](<./Images/L2T3S2.png>)

1. In the hub menu bar on the left, Click on your workspace **Workspace-<inject key="DeploymentID" enableCopy="false"/> (1)**

   ![](<./Images/L2T3S3.png>)

1. Select the **dw semantic model** **(1)** and click on **Open semantic model** **(2)** option to edit the semantic model.

   ![](<./Images/L2T3S4.png>)

1. At the top right corner, Click on **Viewing (1)** to change it to **Editing** **2** permission settings. 

   ![](<./Images/L2T3S5.png>)

1. In the model pane, rearrange the tables in your data warehouse so that the **FactSalesOrder** table is in the middle, like this:

   ![Screenshot of the data warehouse model page.](./Images/L2T3S6.png)

1. Drag the **ProductKey** field from the **FactSalesOrder** table and drop it on the **ProductKey** field in the **DimProduct** table. In the **New relationship** dialog, confirm the following:

   - **From table**: FactSalesOrder **(1)**
   - **Column**: ProductKey **(2)**
   - **To table**: DimProduct **(3)**
   - **Column**: ProductKey **(4)**
   - **Cardinality**: Many to one (*:1) **(5)**
   - **Cross-filter direction**: Single **(6)**
   - **Make this relationship active**: Checked **(7)**
   - Click **Save** **(8)** to create the relationship.

      ![](<./Images/gr_fb_e2_13.png>)

1. Repeat the process to create many-to-one relationships between the following tables:

   - **FactSalesOrder.CustomerKey** &#8594; **DimCustomer.CustomerKey**
   - **FactSalesOrder.SalesOrderDateKey** &#8594; **DimDate.DateKey**

      ![](./Images/L2T3S8.png)

      > **Note**: When all of the relationships have been defined, the model should look like this.

## Task 4: Query data Warehouse tables

Since the data warehouse is a relational database, you can use SQL to query its tables.

### Query fact and dimension tables

Most queries in a relational data warehouse involve aggregating and grouping data (using aggregate functions and GROUP BY clauses) across related tables (using JOIN clauses).

1. In the hub menu bar on the left, Click on your workspace **Workspace-<inject key="DeploymentID" enableCopy="false"/> (1)** and select the **myDataWarehouse** **(2)** warehouse.

   ![](<./Images/L2T4S1.png>)

1. On the **Home** tab, click the dropdown next to **New SQL query** **(1)**, then select **New SQL query** **(2)** to open a new query editor

   ![](<./Images/L1T62.png>)

1. Run the following code:

   ```sql
   SELECT  d.[Year] AS CalendarYear,
           d.[Month] AS MonthOfYear,
           d.MonthName AS MonthName,
          SUM(so.SalesTotal) AS SalesRevenue
   FROM FactSalesOrder AS so
   JOIN DimDate AS d ON so.SalesOrderDateKey = d.DateKey
   GROUP BY d.[Year], d.[Month], d.MonthName
   ORDER BY CalendarYear, MonthOfYear;
   ```

   ![](<./Images/L2T4S3.png>)

   > **Note:** that the attributes in the time dimension enable you to aggregate the measures in the fact table at multiple hierarchical levels - in this case, year and month. This is a common pattern in data warehouses.

1. Modify the query as follows to add a second dimension to the aggregation.

   ```sql
   SELECT  d.[Year] AS CalendarYear,
          d.[Month] AS MonthOfYear,
          d.MonthName AS MonthName,
          c.CountryRegion AS SalesRegion,
         SUM(so.SalesTotal) AS SalesRevenue
   FROM FactSalesOrder AS so
   JOIN DimDate AS d ON so.SalesOrderDateKey = d.DateKey
   JOIN DimCustomer AS c ON so.CustomerKey = c.CustomerKey
   GROUP BY d.[Year], d.[Month], d.MonthName, c.CountryRegion
   ORDER BY CalendarYear, MonthOfYear, SalesRegion;
   ```

1. Run the modified query and review the results, which now include sales revenue aggregated by year, month, and sales region.

   ![](<./Images/L2T4S5.png>)

## Task 5: Create a View, Visual Query and Visualizing data

A data warehouse in Microsoft Fabric has many of the same capabilities you may be used to in relational databases. For example, you can create database objects like views and stored procedures to encapsulate SQL logic.

### Create a View

1. Modify the query you created previously as follows to create a view (note that you need to remove the ORDER BY clause to create a view).

   ```sql
   CREATE VIEW vSalesByRegion
   AS
   SELECT  d.[Year] AS CalendarYear,
          d.[Month] AS MonthOfYear,
          d.MonthName AS MonthName,
          c.CountryRegion AS SalesRegion,
         SUM(so.SalesTotal) AS SalesRevenue
   FROM FactSalesOrder AS so
   JOIN DimDate AS d ON so.SalesOrderDateKey = d.DateKey
   JOIN DimCustomer AS c ON so.CustomerKey = c.CustomerKey
   GROUP BY d.[Year], d.[Month], d.MonthName, c.CountryRegion;
   ```

1. Run the query to create the view. Then refresh the data warehouse schema and verify that the new view is listed in the **Explorer** pane.

1. Create a new SQL query and run the following SELECT statement:

   ```SQL
   SELECT CalendarYear, MonthName, SalesRegion, SalesRevenue
   FROM vSalesByRegion
   ORDER BY CalendarYear, MonthOfYear, SalesRegion;
   ```

### Create a visual query

Instead of writing SQL code, you can use the graphical query designer to query the tables in your data warehouse. This experience is similar to Power Query online, where you can create data transformation steps with no code. For more complex tasks, you can use Power Query's M (Mashup) language.

1. On the **Home** tab, click the dropdown arrow next to **New SQL query** **(1)**, then select **New visual query** **(2)** to open the visual query editor.

   ![Screenshot of a Visual query.](./Images/L1T7S1.png)

1. Drag **FactSalesOrder** onto the **canvas**. Notice that a preview of the table is displayed in the **Preview** pane below.

1. Drag **DimProduct** onto the **canvas**. We now have two tables in our query.

1. Use the **(+)** button on the **FactSalesOrder** table on the canvas to **Merge queries**.

   ![Screenshot of the canvas with the FactSalesOrder table selected.](./Images/visual-query-merge1.png)

1. In the **Merge queries** window, select **DimProduct** **(1)** as the right table for merge, then select the **ProductKey** **(2)** column in both the source table and the right table **(3)**, keep the join kind as **Left outer** **(4)**, and click **OK** **(5)** to proceed.

   ![Screenshot of a Visual query.](./Images/cor_e2_fz_4.png)

1. In the **Preview**, note that the new **DimProduct (1)** column has been added to the FactSalesOrder table. Expand the column by clicking the arrow to the right of the column name. Select **ProductName (2)** and click **OK (3)**.

   ![Screenshot of the preview pane with the DimProduct column expanded, with ProductName selected.](./Images/visual-query-preview1.png)

1. If you're interested in looking at data for a single product, per a manager's request, you can now use the **ProductName** column to filter the data in the query. Filter the **ProductName** column to look at **Cable Lock** data only.

1. From here, you can analyze the results of this single query by selecting **Visualize results** or **Open in Excel**. You can now see exactly what the manager was asking for, so we don't need to analyze the results further.

### Visualize your data

You can easily visualize the data in either a single query or in your data warehouse. Before you visualize, hide columns and/or tables that aren't friendly to report designers.

1. In the hub menu bar on the left, click on your workspace **Workspace-<inject key="DeploymentID" enableCopy="false"/>**.

   ![](<./Images/L2T3S3.png>)

1. Select the **dw semantic model** **(1)** and click on **Open semantic model** **(2)** option to edit the semantic model.

   ![](<./Images/L2T3S4.png>)

1. At the top right corner, Click on **Viewing** **1** to change it to **Editing** **2** permission settings. 

   ![](<./Images/L2T5S11.png>)

1. Hide the following columns in your Fact and Dimension tables that are not necessary to create a report. Note that this does not remove the columns from the model; it simply hides them from view on the report canvas.

   1. FactSalesOrder
      - **SalesOrderDateKey**
      - **CustomerKey**
      - **ProductKey**
   1. DimCustomer
      - **CustomerKey**
      - **CustomerAltKey**
   1. DimDate
      - **DateKey**
      - **DateAltKey**
   1. DimProduct
      - **ProductKey**
      - **ProductAltKey**

      ![](./Images/L2T5S12.png)

1. Now you're ready to build a report and make this dataset available to others.

1. On the **File** tab **(1)**, click on **Create new report** **(2)** to begin creating a new Power BI report.

   ![](<./Images/L2T5S14.png>)

1. In the report canvas, expand the **DimProduct (1)** and **FactSalesOrder (2)** tables from the **Data** pane, then select the **Clustered bar chart** visual **(3)**. Add **Category** to the Y-axis and **SalesTotal** to the X-axis to create a bar chart visual **(4)**.

   ![](<./Images/L2T5S15.png>)

1. To save the report, click **File** **(1)** in the top menu, then select **Save** **(2)**.

   ![](./Images/L1T8S7.png)

1. In the **Save report** dialog box, enter the name **Sales Report (1)** in the text field (1), then click **Save** **(2)** to store the report.

   ![](./Images/L2T5S17.png)

1. In the menu hub on the left, navigate back to the workspace. Notice that you now have three items saved in your workspace: your data warehouse, its default dataset, and the report you created.

    ![](./Images/L2T5S18.png)

## Clean up resources

In this exercise, you have created a data warehouse that contains multiple tables. You used SQL to insert data into the tables and query them. and also used the visual query tool. Finally, you enhanced the data model for the data warehouse's default dataset and used it as the source for a report.

If you've finished exploring your data warehouse, you can delete the workspace you created for this exercise.

1. In the bar on the left, select the icon for your workspace to view all of the items it contains.

   ![](./Images/L2T3S3.png)

2. In the **ellipses** menu on the toolbar, select **Workspace settings**.

   ![](./Images/L2T5S21.png)

3. In the **General (1)** Pane, Under the **Delete workspace** section, click **Remove this workspace (2)**.

   ![](./Images/deleteworkspace.png)

## Summary

In this exercise, you have accomplished the following:

- Created a Data Warehouse.
- Created tables and inserted sample data.
- Defined a Data Model to support reporting.
- Queried data from the Data Warehouse tables.
- Created a View, performed Visual Queries, and visualized the data.

## Congratulations! You have successfully completed the Hands-on lab.
