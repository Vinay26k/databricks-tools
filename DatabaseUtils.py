from multiprocessing.pool import ThreadPool

def publishTable(db, tbl, path, completed, failed, tracker):
  try:
    if tbl not in tracker:
      spark.sql(f'SELECT * FROM {db}.{tbl}').write.mode('OVERWRITE').parquet(path+tbl)
      completed.append(tbl)
  except Exception as e:
    failed[tbl] = str(e)
  finally:
    tracker[tbl]= 'Processed'

def publishTables(db, path):
  tables = [x['tableName'] for x in spark.sql(f'SHOW TABLES IN {db}').collect()]
  total = len(tables)
  pool = ThreadPool(total)
  print(f"Total {total} tables found in {db}")
  completed, failed, tracker = [], {}, {}
  try:
    pool.map(lambda tbl: publishTable(db, tbl, path, completed, failed, tracker), tables)
  except Exception as e:
    failed[tbl]=str(e)
  finally:
    pool.close()
    pool.join()
  return total, completed, failed
