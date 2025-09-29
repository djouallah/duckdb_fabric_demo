# Fabric Demo - Australian Electricity Market Data

A ready-to-use solution for ingesting and analyzing Australian electricity market data in Microsoft Fabric.

## 📋 Overview

This repository provides an automated data pipeline for the Australian electricity market. Simply download the notebook and run it in your Microsoft Fabric workspace to start collecting and analyzing energy market data.

## 🎯 What You'll Get

- **Automated Data Collection**: Fetch Australian electricity market data automatically
- **Incremental Data Loading**: Loads historical data progressively to handle GitHub API limits and notebook session timeouts
- **Scheduled Updates**: Keep your data fresh with automated daily/5 minutes updates
- **Ready for Analysis**: Data is immediately available for Power BI reports and analysis

## 📊 About the Data

The data comes from the Australian Energy Market Operator (AEMO), covering the National Electricity Market (NEM) across Queensland, New South Wales, Victoria, South Australia, and Tasmania.

The dataset includes:
- **Market Prices**: Electricity spot prices by region
- **Demand Data**: Power consumption patterns
- **Generation Data**: Energy production by source
- **Regional Breakdowns**: Data for each Australian state in the NEM

## 🚀 How to Use

### Step 1: Import the Notebook
1. Download the Python Notebook (simple_orchestrator) from this repo, notice, it is a pure python notebook and does not require spark
2. Import it to Fabric Workspace


### Step 2: Update Parameters

Before running the notebook, you need to configure it for your environment:

1. Open the notebook in your Fabric workspace
2. Find the parameters section at the top
3. Update the following values:
   - **Workspace**: Your Fabric workspace name (no spaces)
   - **Lakehouse**: Your lakehouse name (no spaces)
   - **Schema**: Your schema name (no spaces)
4. **⚠️ IMPORTANT**: Ensure none of these names contain spaces
   - Use underscores instead: `my_workspace` not `my workspace`

     <img width="1352" height="193" alt="image" src="https://github.com/user-attachments/assets/c26546a7-29e3-49a3-93ba-d73724bc27e3" />



### Step 3: Run the Notebook

1. After updating the parameters, click **Run All**
2. The notebook will load historical data incrementally
3. **Note**: Due to GitHub API rate limits and Fabric's 1-hour notebook token timeout, the data loads progressively
4. Each run will pick up where it left off - no need to worry about duplicates
5. Monitor the progress to ensure it completes successfully

### Step 4: Schedule Automatic Updates

To keep your data updated and continue loading historical data:

1. In your Fabric workspace, create a new **Pipeline**
2. Add a **Notebook activity** to the pipeline
3. Select the notebook you uploaded
4. Configure the schedule (recommended: run every hour or daily, or 5 minutes)
   - For initial backfill: First run take around 17 minutes then 7 minutes once completed it will take around 1 minutes to load 5 minutes data
5. **⚠️ IMPORTANT**: Set **Concurrency to 1** in the pipeline settings
   - This ensures only one instance runs at a time
   - Prevents data duplication and conflicts
6. Save and activate the pipeline

### Why Concurrency = 1?

Setting concurrency to 1 means only one copy of the pipeline runs at any time. This is critical because:
- Prevents duplicate data loading
- Avoids conflicts when updating the same data
- Ensures data consistency and accuracy
- Allows the incremental load process to work correctly

## 📊 Next Steps After Setup

Once your data is loading:

- **Import Power BI Reports**
- Change parameters in semantic model setting:
 <img width="389" height="492" alt="image" src="https://github.com/user-attachments/assets/abedcb95-d28c-433a-9568-aa034b6d2a91" />



## 🔧 Common Use Cases

- **Energy Cost Analysis**: Track and forecast electricity costs
- **Demand Forecasting**: Predict future energy consumption
- **Market Intelligence**: Understand pricing patterns and market dynamics
- **Operational Planning**: Plan energy usage during low-price periods
- **Sustainability Reporting**: Monitor renewable energy generation

## 📚 Helpful Resources

### Microsoft Fabric Documentation
- [Getting Started with Fabric](https://learn.microsoft.com/en-us/fabric/get-started/)
- [Working with Python Notebooks](https://learn.microsoft.com/en-us/fabric/data-engineering/using-python-experience-on-notebook)
- [Creating Data Pipelines](https://learn.microsoft.com/en-us/fabric/data-factory/create-first-pipeline-with-sample-data)

### Australian Energy Market
- [AEMO Official Website](https://aemo.com.au/)
- [Understanding the NEM](https://aemo.com.au/en/energy-systems/electricity/national-electricity-market-nem)
