# Databricks notebook source
from multiprocessing.pool import ThreadPool

# COMMAND ----------

def extractTable_(path, tbl, db, completed, failed):
  try:
    if tbl not in completed and tbl!='Published.txt':
      spark.sql(f'DROP TABLE IF EXISTS {db}.{tbl}')
      spark.sql(f'''
        CREATE TABLE {db}.{tbl}
        AS
        SELECT * FROM parquet.`{path}`
      ''')
      completed.append(tbl)
  except:
    failed.append(tbl)

# COMMAND ----------

def extractTables(path, db):
  files = [x.path for x in dbutils.fs.ls(path)]
  print(f"Total: {len(files)}")
  path_files = stg_files = [(x, x.split('/')[-1]) if x[-1]!='/' else (x, x.split('/')[-2]) for x in files]
  pool = ThreadPool(len(stg_files))
  completed, failed = [], []
  try:
    pool.map(lambda x: extractTable_(x[0], x[1], db, completed, failed), path_files)
  finally:
    pool.close()
    pool.join()
    return completed, failed
