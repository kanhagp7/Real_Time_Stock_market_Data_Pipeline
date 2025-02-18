# Real_Time_Stock_market_Data_Pipeline

### **Overview**  
This project implements a **real-time stock market ETL pipeline** that fetches live stock data, processes it in **Apache Spark**, and stores it in **MySQL** for further analysis. The pipeline is **fully automated** using **Apache Airflow**, ensuring it runs during NASDAQ market hours and stops automatically at closing time.  

### **Technologies Used**
- **Python** → Core programming language for data processing.  
- **WebSockets** → Fetches **real-time stock market data**.  
- **Apache Kafka** → Message queue for streaming stock data.  
- **Apache Spark** → Processes the streaming data in real time.  
- **MySQL** → Stores processed stock data for analysis.  
- **Apache Airflow** → Automates & schedules the ETL pipeline.  
- **Power BI** → Creates interactive dashboards for visualizing stock data trends.

### **Project Workflow**
1️ **Data Ingestion**  
   - A **WebSocket client** continuously fetches **live stock prices**.  
   - Data is **published to a Kafka topic** in real time.  

2️ **Data Processing**  
   - A **Spark Structured Streaming consumer** reads data from Kafka.  
   - The data is **transformed** (e.g., filtering, aggregations).  
   - The processed data is **stored in MySQL**.  

3️ **Pipeline Automation with Airflow**  
   - **Producer & Consumer scripts** start at **NASDAQ market open (9:30 AM ET / 8:00 PM IST)**.  
   - The pipeline **runs continuously during market hours**.  
   - At **market close (4:00 PM ET / 1:30 AM IST)**, Airflow **automatically stops the pipeline**.  

4 **Live Dashboard using PowerBI**
   - Connects to MySQL to fetch processed stock data.
   - Displays interactive charts and real-time trends of stock prices.
   - Enables data-driven decision-making through visual insights.
     
