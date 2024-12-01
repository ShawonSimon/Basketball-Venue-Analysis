# Basketball Venues Data Engineering Project

## Project Overview

This project showcases a comprehensive data engineering workflow that encompasses data extraction, cleaning, transformation, and visualization. The objective was to extract data from [List of basketball arenas](https://en.wikipedia.org/wiki/List_of_basketball_arenas) worldwide, cleanse and transform the data, and load it into Azure Data Lake for further analysis in Azure Synapse Analytics. Finally, the insights were visualized using Tableau, providing valuable analytics on the global distribution and characteristics of basketball venues.

## Project Achitecture
![](https://github.com/ShawonSimon/Basketball-Venue-Analysis/blob/main/screenshots/projectAchitecture.jpg?raw=true)

## Technologies Used

This project utilizes the following technologies: 

 - Python: For data extraction and transformation.
 - BeautifulSoup and Requests: For web scraping.
 - Azure Data Lake: For scalable storage
 - Geopy: For geocoding.
 - Azure Synapse Analytics: For data warehousing and analytics.
 - Tableau: For creating visualization dashboards.
 - Apache Airflow: For orchestrating the workflow.
 - Azure Virtual Machine: For hosting the Airflow instance and running the pipeline.

## Data Extraction

Data was extracted from a Wikipedia page using Python's web scraping libraries:
 
 - Requests: For fetching the HTML content of the Wikipedia page.
 - BeautifulSoup: For parsing and extracting relevant data tables from the HTML content.
   The extracted data includes information such as venue names, locations, seating capacities, and associated teams/events.
 
The extracted data includes key attributes like:
 
 - Venue name
 - Seating capacity
 - Location
 - Associated basketball teams
 - Image links

## Data Transformation

 After extraction, the raw data was cleaned and transformed to ensure quality and consistency.

## Cleaning Processes

  - Cleaned and normalized text fields by removing special characters and unnecessary artifacts.
  - Handled edge cases such as missing data or extra annotations in the text.
  - Fetched latitude and longitude for venue locations using the MapQuest API.
  - Appended a default placeholder image for missing image fields.

 ## Data Loading and Analytics

  1. ## Data Lake Storage

  The transformed data was uploaded to Azure Data Lake, leveraging its scalable storage for big data analytics.

  2. ## Azure Synapse Analytics

  The data was moved from Azure Data Lake to Azure Synapse Analytics. Here, the data was integrated into a structured database for OLAP operations. Using Synapse's SQL capabilities, 
  queries and aggregations were run to generate meaningful insights, such as:

  - Regional distribution of basketball venues.
  - Capacity trends across different countries.

 ## Visualization

 The processed data was visualized using Tableau. A dashboard was created to:

  - Display the global distribution of basketball venues on a map.
  - Highlight top venues by seating capacity.
  - Provide interactive filters for region-specific analysis.

 ## Orchestration

 The entire workflow was orchestrated using Apache Airflow. Key components included:
  - DAGs for scheduling and managing tasks.
  - PythonOperators for implementing data extraction and transformation logic.
  
  This orchestration ensured efficient and automated execution of the pipeline.

  Many thanks to [Yusuf Ganiyu](https://github.com/airscholar) for inspiring this project.

