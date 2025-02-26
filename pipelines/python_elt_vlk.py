import os
import subprocess
import pandas as pd
from datetime import datetime as dt
from dotenv import load_dotenv
from sqlalchemy import create_engine,text

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials from environment variables
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Construct the DB connection string
DB_CONN = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@localhost:5432/{DB_NAME}"

# Declaring Data and DBT model paths 
data_folder_path = 'pipelines\data'

current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
dbt_model_path = os.path.join(parent_dir, 'de_assessment\\vlk_assessment')



# Step 1: Extract Data (Load CSV into Bronze Table)
def ingest_data(data_folder):
    engine = create_engine(DB_CONN)
    

    for file_name in os.listdir(data_folder):
        file_path = os.path.join(data_folder, file_name)
        
        if not os.path.isfile(file_path):
            continue  # Skip non-file entries
        
        try:
            
            dataset_name = file_name.split('.')[0]

            df = pd.read_csv(file_path)
            df['load_date'] = dt.now()
            df['source_file'] = file_name
            
            table_name = f"{dataset_name.lower()}_bronze_layer"
            df.to_sql(table_name, engine, if_exists="append", index=False, schema="bronze_schema")
            
            print(f"✅ Data Ingested into Bronze Layer: {table_name}")
        except Exception as e:
            print(f"❌ Error processing {file_name}: {e}")




# Step 2: Clean & Load Data into Silver Table

def clean_and_load():
    engine = create_engine(DB_CONN)
    with engine.connect() as connection:
        result = connection.execute(text("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'bronze_schema' AND tablename LIKE '%_bronze_layer'
        """))
        
        bronze_tables = [row[0] for row in result]
        
        for table in bronze_tables:
            df = pd.read_sql(f"SELECT * FROM bronze_schema.{table}", engine)
            
            # Table specific data quality improvements
            if 'transactions' in table.lower():
                df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
                df.drop_duplicates(subset=["transaction_id", "client_id", "transaction_amount", "transaction_date", "country"],keep='last',inplace=True)
            
            if 'clients' in table.lower():
                df.drop_duplicates(subset=["client_id", "name","city", "country"],keep='last',inplace=True)

            pre_silver_table = table.replace("_bronze_layer", "_pre_silver_layer")
            df.to_sql(pre_silver_table, engine, if_exists="replace", index=False, schema="silver_schema")
            print(f"✅ Data Cleaned and Loaded into Pre Silver Layer: {pre_silver_table}")


def deploy_silver_layer_data_models():
    # Run DDL to deploy Data models in the database
    engine = create_engine(DB_CONN)
    with engine.connect() as connection:
        result = connection.execute(text("""
            BEGIN;

CREATE TABLE IF NOT EXISTS silver_schema.clients_silver_layer
(
    client_id integer NOT NULL,
    name character varying(100),
    age integer,
    city character varying(100),
    country character varying(100),
    contact_number integer,
    load_date timestamp without time zone,
    source_file character varying(100),
    PRIMARY KEY (client_id)
);

CREATE TABLE IF NOT EXISTS silver_schema.transactions_silver_layer
(
    transaction_id integer NOT NULL,
    client_id integer,
    transaction_amount integer,
    transaction_date timestamp without time zone,
    country character varying(100),
    load_date timestamp without time zone,
    source_file character varying(100),
    PRIMARY KEY (transaction_id)
);

CREATE TABLE IF NOT EXISTS silver_schema.risk_event_silver_layer
(
    risk_event_id integer NOT NULL,
    client_id integer,
    transaction_id integer,
    risk_reason_id integer,
    risk_score integer,
    event_timestamp timestamp without time zone,
    PRIMARY KEY (risk_event_id),
    UNIQUE (risk_reason_id)

);

CREATE TABLE IF NOT EXISTS silver_schema.risk_factor_silver_layer
(
    risk_reason_id integer,
    risk_reason_name character varying(50),
    PRIMARY KEY (risk_reason_id)
);

ALTER TABLE IF EXISTS silver_schema.transactions_silver_layer
    ADD FOREIGN KEY (client_id)
    REFERENCES silver_schema.clients_silver_layer (client_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE IF EXISTS silver_schema.risk_event_silver_layer
    ADD FOREIGN KEY (client_id)
    REFERENCES silver_schema.clients_silver_layer (client_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE IF EXISTS silver_schema.risk_event_silver_layer
    ADD FOREIGN KEY (transaction_id)
    REFERENCES silver_schema.transactions_silver_layer (transaction_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE IF EXISTS silver_schema.risk_factor_silver_layer
    ADD FOREIGN KEY (risk_reason_id)
    REFERENCES silver_schema.risk_event_silver_layer (risk_reason_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

END;
        """))
        print("Data Models Deployed")
    return


# Step 3: Run DBT to fully construct the Silver Layer

def run_dbt_silver():
    print("Running dbt models in : ",dbt_model_path)
    result = subprocess.run(["dbt", "run","--select","tag:silver_layer"], cwd=dbt_model_path, capture_output=True, text=True)
    if result.returncode != 0:
        print("❌ Error processing Silver layer load!", result.stdout)
        return False
    else:
        print("✅ Data Ingested into Silver Layer \n", result.stdout)
        return True
    


# Step 3: Run DBT to Transform (Gold Layer )

def run_dbt_gold():
    result = subprocess.run(["dbt", "run" ,"--select","tag:gold_layer","-t","prod"], cwd=dbt_model_path, capture_output=True, text=True)
    if result.returncode != 0:
        print("❌ Error processing Gold layer load!", result.stdout)
        return False
    else:
        print("✅ DBT Transformation Completed\n", result.stdout)
        return True
    


# Step 4: Run DBT Tests (For Validation)
def run_dbt_tests():
    result = subprocess.run(["dbt", "test" ,"--select" ,"tag:silver_layer" ,"tag:gold_layer"], cwd=dbt_model_path, capture_output=True, text=True)
    if result.returncode != 0:
        print("❌ DBT Test Failures!", result.stdout)
        return False
    else:
        print("✅ DBT Tests Completed\n", result.stdout)
        return True
    

# Run ELT Pipeline
if __name__ == "__main__":
    ingest_data(data_folder_path)
    clean_and_load()
    deploy_silver_layer_data_models()
    run_dbt_silver()
    run_dbt_gold()
    run_dbt_tests()
