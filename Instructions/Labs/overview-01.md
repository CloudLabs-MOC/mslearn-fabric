# Work with Data Lake and Data Factory Pipelines in Microsoft Fabric​

### Overall Estimated Duration: 4 Hours

## Overview

This lab provides hands-on experience in working with Microsoft Fabric Lakehouses and Data Factory Pipelines to store, manage, and process data within a unified analytics platform. You will learn how to organize and manage structured data using Delta Lake tables in a Lakehouse environment and perform data processing using Apache Spark through notebooks.

Throughout the lab, you will build automated data ingestion workflows using Fabric Data Factory Pipelines to extract, transform, and load (ETL) data from various sources into the Lakehouse. Using the graphical pipeline canvas, you will create and manage data workflows with minimal coding effort.

By integrating data storage, processing, and orchestration capabilities, this lab helps you understand how Microsoft Fabric supports scalable data management and enables you to build efficient end-to-end data engineering solutions for analytics and reporting.

## Objective

The objective is to efficiently manage and analyze data by leveraging Data Lake for scalable storage and Data Factory Pipelines in Microsoft Fabric to automate ETL workflows and streamline data processing.

- **Use Delta tables in Apache Spark:** Leverage Delta tables in Apache Spark to enable reliable, scalable, and performant data processing with ACID transactions and versioning for advanced analytics.
- **Ingest data with a pipeline:** Automate data ingestion by building pipelines to efficiently extract, transform, and load (ETL) data from various sources into a data lake or warehouse in Microsoft Fabric.

## Prerequisites

Participants should have:

- **Basic Data Engineering Knowledge:** Understanding of data storage, processing, and ETL concepts, including data lakes and warehouses.
- **Experience with Microsoft Fabric:** Familiarity with the Microsoft Fabric platform, including lakehouses, tables, and pipelines.
- **SQL Proficiency:** Strong skills in SQL for querying and managing Delta tables.
- **Understanding of Apache Spark:** Basic knowledge of Spark architecture, DataFrames, and Spark SQL for distributed data processing.
- **Familiarity with Data Pipelines:** Understanding of ETL processes and experience in building and managing data pipelines.

## Architechture

The architecture of a Microsoft Fabric lab integrates Data Lake storage with Delta tables for reliable data storage, while Data Factory Pipelines automate ETL processes. Apache Spark powers distributed data processing on Delta tables, and notebooks enable interactive analytics. The environment supports data consistency, scalability, and integration with cloud services, ensuring secure and efficient data management and processing.

## Architecture Diagram

![](./Images/arch-diag-2702.png)

## Explanation of Components

The architecture for this lab involves the following key components:

- **Workspaces:** Collaborative environments in Microsoft Fabric where users can organize and manage data, notebooks, and pipelines for seamless data analysis and processing.
- **Delta Lake:** A storage layer that enables ACID transactions, versioning, and scalable data management on top of Data Lake for reliable data processing.
- **Tables:** Structured data containers in Delta Lake that store processed data and support querying and analytics with features like schema enforcement and time travel.
- **Notebooks:** Interactive environments where users write and execute code (e.g., Python, Scala) to analyze data, build models, and visualize results.
- **Pipelines:** Automated workflows that orchestrate the extraction, transformation, and loading (ETL) of data across various sources and destinations within the Fabric ecosystem.
- **Apache Spark:** A distributed processing engine that enables scalable data analytics and transformation on large datasets within the Microsoft Fabric platform.
- **Data Lake:** A centralized storage system that stores large volumes of raw, unstructured, and structured data for further processing and analysis.

## Getting Started with the Lab

Welcome to your Work with Data Lake and Data Factory Pipelines in Microsoft Fabric​ Workshop! We've prepared a seamless environment for you to explore and learn about Azure services. Let's begin by making the most of this experience:

## Accessing Your Lab Environment
 
Once you're ready to dive in, your virtual machine and lab guide will be right at your fingertips within your web browser.

![](./Images/fab-gs-ms1-g1.png)

### Virtual Machine & Lab Guide
 
Your virtual machine is your workhorse throughout the workshop. The lab guide is your roadmap to success.
 
## Exploring Your Lab Resources
 
To get a better understanding of your lab resources and credentials, navigate to the **Environment** tab.
 
![Explore Lab Resources](./Images/exp-res-2302.png)
 
## Utilizing the Split Window Feature
 
For convenience, you can open the lab guide in a separate window by selecting the **Split Window** button from the Top right corner.
 
![Use the Split Window Feature](./Images/split-2302.png)
 
## Managing Your Virtual Machine
 
You can **Start, Stop, Restart (2)** your virtual machine from the **Resources (1)** tab.

![Manage Your Virtual Machine](./Images/gt-004.png)

## Lab Guide Zoom In/Zoom Out

To adjust the zoom level of the lab guide, select the **A↕ (1)** icon and choose your preferred zoom **percentage (2)**.

![](./Images/ns-fab-g5.png)

## Support Contact
 
The CloudLabs support team is available 24/7, 365 days a year, via email and live chat to ensure seamless assistance at any time. We offer dedicated support channels tailored specifically for both learners and instructors, ensuring that all your needs are promptly and efficiently addressed.

Learner Support Contacts:
- Email Support: cloudlabs-support@spektrasystems.com
- Live Chat Support: https://cloudlabs.ai/labs-support

Now, click on **Next** from the lower right corner to move on to the next page.

![](./Images/ns-fab-g1.png)

### Happy Learning!!
