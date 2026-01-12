📈 Scala Spark Finance Analytics

This project implements an advanced Data Engineering pipeline for financial market analysis. Leveraging the Scala and Apache Spark technology stack, the system processes historical stock data, calculates technical indicators (SMA), and persists the results in a Data Lake based on Delta Lake.

🏗️ Medallion Architecture

The project adopts the medallion architecture to ensure data quality and traceability:

Raw Layer (Bronze): Ingestion of raw CSV/TXT files (e.g., zion.us.txt).

Silver Layer: Cleaned, typed data enriched with 50-day and 200-day simple moving averages.

Storage: Delta Lake (Parquet + Transaction Log).

Local Path: C:\Users\ANTO\Documents\progetti\ScalaSparkFinanceProject\data\silver

Gold Layer (Reporting): Final visualization via auto-generated interactive dashboards.

🛠️ Technology Stack

Language: Scala 2.12.x

Compute Engine: Apache Spark 3.x (Spark SQL, Window Functions)

Storage: Delta Lake (for ACID transactions and Time Travel)

Visualization: Plotly.js (Interactive HTML Dashboard)

Environment: Windows with native Hadoop integration (winutils.exe)

🚀 Key Features

Dynamic Ticker Injection: Ability to process files lacking metadata by inserting the Ticker via programmed ingestion logic.

Metric Calculation with Window Functions: Use of partitioned time windows (Window.partitionBy) to calculate moving averages without mixing data from different stocks.

Windows Optimization: Specific configuration to overcome Hadoop limitations on Windows through the integration of winutils.exe in C:\hadoop.

Interactive Dashboard: Automatic generation of an HTML file with navigable charts and range sliders upon completion of each run.

📁 Module Structure

BatchProcessor: The core of the pipeline; coordinates reading, transformation, and writing.

MetricCalculator: Mathematical module for calculating Moving Averages (SMA) using Spark DSL.

Visualizer: HTML report generator with interactive charts and JSON data injection.

SparkConfig: Centralizes the creation of the SparkSession and necessary extensions for Delta Lake.

⚙️ Configuration and Execution

Local Requirements

Hadoop Home: The project requires winutils.exe and hadoop.dll located in C:\hadoop\bin.

Dataset: Place source files in data/raw/Stocks/.

Commands

To start the pipeline via SBT:

sbt run