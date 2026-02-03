# Module 2 Homework Solution

This repository contains the complete solution for the Data Engineering Zoomcamp Module 2 homework on workflow orchestration with Kestra.

## Overview

The goal of this homework is to extend the existing data processing flows to include data for the year 2021. This solution provides:

1.  **Quiz Answers:** The answers to the 6 quiz questions.
2.  **Extended Kestra Flows:** YAML files for Kestra flows that can process taxi data for 2021.
3.  **Docker Setup:** A `docker-compose.yml` file to easily set up the required environment with Kestra, PostgreSQL, and pgAdmin.


## Code and Implementation

### Directory Structure

```
.
├── docker-compose.yml
├── flows
│   ├── homework_taxi_2021.yaml
│   ├── homework_taxi_2021_scheduled.yaml
│   └── homework_taxi_2021_foreach.yaml
├── init-db.sql
└── README.md
```

### How to Run

1.  **Start the Environment:**

    Run the following command in your terminal from the root of this directory:

    ```bash
    docker-compose up -d
    ```

    This will start Kestra, a PostgreSQL database, and pgAdmin.

2.  **Access Kestra:**

    Open your web browser and navigate to `http://localhost:8080`.

3.  **Import and Run the Flows:**

    You can import the flows from the `flows/` directory into the Kestra UI and run them. Here is a description of each flow:

    *   `homework_taxi_2021.yaml`: A manual flow to process a single month of taxi data for a specific year (including 2021). You can select the taxi type, year, and month from the UI inputs.

    *   `homework_taxi_2021_scheduled.yaml`: A scheduled flow that can be used to backfill data for 2021. To do this, go to the "Triggers" tab in the Kestra UI, select the trigger, and use the "Backfill" option to specify the date range from `2021-01-01` to `2021-07-31`.

    *   `homework_taxi_2021_foreach.yaml`: A bonus flow that demonstrates how to use the `ForEach` task to loop through all the required months of 2021 for both green and yellow taxi data, and process them in a single workflow execution.
