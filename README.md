# Snowflake ETL Project

This project demonstrates an ETL (Extract, Transform, Load) pipeline using Snowflake for analyzing gaming data. The goal is to understand user gaming patterns to provide recommendations for game development.

## Overview

In this project, I have:

1. Extracted data from JSON files.
2. Transformed the data by normalizing it, adding local time zones based on IP addresses, and calculating timestamps in each gamer's local time zone.
3. Loaded the enhanced data into Snowflake tables for further analysis.

## Project Details

### Data Extraction

Data was extracted from JSON files stored in Snowflake stages.

```sql
-- List the files in the Snowflake stage

-- Copy data from the stage into a Snowflake table

