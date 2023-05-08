import pandas as pd 
import numpy as np
import duckdb

#to create a persisted DB with duckdb use 
conn = duckdb.connect('../datapond/cms.db', read_only=False)

#create tables
conn.sql("CREATE TABLE mbsf(DESYNPUF_ID VARCHAR(20) PRIMARY KEY, BENE_BIRTH_DT DATE, BENE_SEX_IDENT_CD INT, BENE_RACE_CD INT)")
conn.sql("CREATE TABLE enroll(DESYNPUF_ID VARCHAR(20), YEAR INT, PRIMARY KEY (DESYNPUF_ID, YEAR), SP_STATE_CODE INT, BENE_HMO_CVRAGE_TOT_MONS INT)")
conn.sql("CREATE TABLE hosp(CLM_ID VARCHAR(20) PRIMARY KEY, DESYNPUF_ID VARCHAR(20), CLM_ADMSN_DT DATE)")

#ingest data
conn.sql("COPY mbsf FROM '../datapond/mbsf.parquet'")
conn.sql("COPY enroll FROM '../datapond/enroll.parquet'")
conn.sql("COPY hosp FROM '../datapond/hosp.parquet'")