\# Azure Customer Lakehouse



An end-to-end Data Engineering project built using \*\*PySpark, Apache Airflow, Delta Lake, Docker, and a Bronze-Silver-Gold architecture\*\*.



The project demonstrates data ingestion, transformation, Delta Lake processing, data quality validation, and audit/reconciliation in an orchestrated pipeline.



\## Architecture



```text

&#x20;                   Raw Customer Data

&#x20;                          |

&#x20;                          v

&#x20;                   +-------------+

&#x20;                   |   Bronze    |

&#x20;                   |   PySpark   |

&#x20;                   +-------------+

&#x20;                          |

&#x20;                          v

&#x20;                   +-------------+

&#x20;                   |   Silver    |

&#x20;                   | Cleaning \&   |

&#x20;                   | Deduplication|

&#x20;                   +-------------+

&#x20;                          |

&#x20;                          v

&#x20;                   +-------------+

&#x20;                   | Delta Silver|

&#x20;                   |  Delta Lake |

&#x20;                   +-------------+

&#x20;                          |

&#x20;                          v

&#x20;                   +-------------+

&#x20;                   |    Gold     |

&#x20;                   | Business    |

&#x20;                   | Transform.  |

&#x20;                   +-------------+

&#x20;                          |

&#x20;                          v

&#x20;                 +-------------------+

&#x20;                 |   Data Quality    |

&#x20;                 | Null/Duplicate/   |

&#x20;                 | Status Validation |

&#x20;                 +-------------------+

&#x20;                          |

&#x20;                          v

&#x20;                 +-------------------+

&#x20;                 | Audit \&           |

&#x20;                 | Reconciliation    |

&#x20;                 +-------------------+



&#x20;                 Orchestrated by

&#x20;                 Apache Airflow

```



\## Technology Stack



\* Python

\* PySpark

\* Apache Airflow

\* Delta Lake

\* Docker

\* PostgreSQL

\* Apache Spark

\* Git \& GitHub

\* Azure Data Engineering concepts



\## Pipeline Flow



The Airflow DAG contains six sequential tasks:



```text

Bronze Ingestion

&#x20;      ↓

Silver Transformation

&#x20;      ↓

Create Delta Silver

&#x20;      ↓

Create Customer Gold

&#x20;      ↓

Data Quality

&#x20;      ↓

Audit \& Reconciliation

```



\## 1. Bronze Layer



The Bronze layer ingests raw customer CSV data using PySpark and stores it as Parquet.



Responsibilities:



\* Read raw CSV files

\* Infer source schema

\* Preserve raw data structure

\* Write data to the Bronze layer



Location:



```text

data/bronze/customers

```



\## 2. Silver Layer



The Silver layer performs data cleansing and standardization.



Transformations include:



\* Null validation

\* Removing invalid customer records

\* Trimming string columns

\* Standardizing customer status

\* Deduplicating customers

\* Keeping the latest record using `updated\_at`

\* Adding processing timestamps



Location:



```text

data/silver/customers

```



\## 3. Delta Lake



The cleansed Silver data is converted into a Delta Lake table.



Delta Lake provides:



\* ACID transactions

\* Schema enforcement

\* Version history

\* Time travel

\* Reliable data processing



Location:



```text

data/silver\_delta/customers

```



\## 4. Gold Layer



The Gold layer creates business-ready customer data.



Derived fields include:



\* `full\_name`

\* `signup\_year`

\* `customer\_age\_in\_days`

\* `is\_active`



The pipeline also generates customer-level aggregations by:



\* City

\* State

\* Customer status



Location:



```text

data/gold/customers

```



\## 5. Data Quality



Automated data-quality checks are executed before the pipeline completes.



The following checks are performed:



\* Null customer IDs

\* Null or blank emails

\* Duplicate customer IDs

\* Invalid customer statuses

\* Null signup dates



Example result:



```text

Total records              : 10

Null customer IDs          : 0

Null emails                : 0

Duplicate customer IDs     : 0

Invalid statuses           : 0

Null signup dates          : 0



Overall Data Quality       : PASSED

```



If a data-quality check fails, the Airflow task fails.



\## 6. Audit \& Reconciliation



The pipeline performs source-to-target reconciliation using Delta Lake version history.



It validates:



```text

Expected Target

=

Target Before

\+ Inserts

\- Deletes

```



The audit process tracks:



\* Source record count

\* Target count before processing

\* Target count after processing

\* Insert count

\* Update count

\* Delete count

\* Data-quality status

\* Reconciliation status

\* Overall pipeline status

\* Pipeline execution timestamp



Audit records are stored as a Delta table:



```text

data/audit/pipeline\_audit

```



\## Airflow DAG



DAG name:



```text

customer\_pipeline

```



Tasks:



```text

bronze\_ingestion

&#x20;       ↓

silver\_transformation

&#x20;       ↓

create\_delta\_silver

&#x20;       ↓

create\_customer\_gold

&#x20;       ↓

data\_quality

&#x20;       ↓

audit\_reconciliation

```



The pipeline is designed so that downstream tasks execute only when upstream tasks succeed.



\## Project Structure



```text

azure-customer-lakehouse/

│

├── airflow/

│   └── dags/

│       └── customer\_pipeline.py

│

├── data/

│   └── raw/

│       ├── customers.csv

│       ├── customers\_batch2.csv

│       ├── customers\_batch3.csv

│       └── customers\_cdc\_batch.csv

│

├── src/

│   ├── ingestion/

│   │   └── read\_customers.py

│   │

│   ├── bronze/

│   │   └── write\_customers\_bronze.py

│   │

│   ├── silver/

│   │   ├── clean\_customers\_silver.py

│   │   ├── create\_delta\_silver.py

│   │   ├── merge\_customers\_delta.py

│   │   ├── process\_customer\_cdc.py

│   │   └── process\_customer\_incremental.py

│   │

│   ├── gold/

│   │   └── create\_customer\_gold.py

│   │

│   ├── quality/

│   │   └── customer\_data\_quality.py

│   │

│   └── audit/

│       └── reconciliation\_audit.py

│

├── Dockerfile

├── docker-compose.yml

├── .gitignore

└── README.md

```



\## Running the Project



\### 1. Clone the repository



```bash

git clone https://github.com/AnandBagate/azure-customer-lakehouse.git

cd azure-customer-lakehouse

```



\### 2. Start Airflow



```bash

docker compose up -d --build

```



\### 3. Verify the DAG



```bash

docker compose exec airflow-dag-processor airflow dags list

```



\### 4. Trigger the pipeline



```bash

docker compose exec airflow-scheduler airflow dags trigger customer\_pipeline

```



\### 5. Check the pipeline



Open the Airflow UI and monitor:



```text

Bronze → Silver → Delta → Gold → DQ → Audit

```



\## Key Data Engineering Concepts Demonstrated



\* ETL pipeline development

\* PySpark transformations

\* Bronze-Silver-Gold architecture

\* Delta Lake

\* Data deduplication

\* Incremental processing concepts

\* CDC processing concepts

\* Data quality validation

\* Source-to-target reconciliation

\* Audit logging

\* Airflow orchestration

\* Docker containerization

\* Pipeline failure handling

\* Git/GitHub version control



\## Project Summary



> Built an end-to-end Data Engineering pipeline using PySpark and Apache Airflow following a Bronze-Silver-Gold architecture. Implemented Delta Lake for reliable storage and versioning, automated data-quality checks, and source-to-target reconciliation with audit logging. The complete pipeline is containerized using Docker and orchestrated through Airflow.



\## Future Enhancements



\* Azure Data Lake Storage Gen2 integration

\* Azure Databricks deployment

\* Azure Data Factory orchestration

\* Real-time CDC processing

\* Incremental watermark-based processing

\* Power BI reporting layer

\* CI/CD using GitHub Actions

\* Cloud-based monitoring and alerting



\## Author



\*\*Anand Bagate\*\*



Data Engineer | PySpark | Azure | Databricks | SQL | Airflow



