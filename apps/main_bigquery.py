from pyspark.sql import SparkSession
from pyspark.sql.functions import col,date_format
import base64
def to_base64(creds):
    creds_bytes = creds.encode('ascii')
    base64_bytes = base64.b64encode(creds_bytes)
    return base64_bytes.decode('ascii')

def init_spark():
  spark_session = SparkSession.builder\
    .appName("trip-app-1")\
    .config("credentialsFile", "/opt/spark-apps/keyfile.json")\
    .config("spak.hadoop.fs.gs.auth.service.account.enable", "true")\
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", '/opt/spark-apps/keyfile.json')\
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
    .config("spak.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")\
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")\
    .getOrCreate()
  return spark_session


def main():
  spark_session = init_spark()

  # df = sql.read.load(file,format = "csv", inferSchema="true", sep="\t", header="true"
  #     )
      # .withColumn("report_hour",date_format(col("time_received"),"yyyy-MM-dd HH:00:00")) \
      # .withColumn("report_date",date_format(col("time_received"),"yyyy-MM-dd"))
  
  # Filter invalid coordinates
  # df.where("latitude <= 90 AND latitude >= -90 AND longitude <= 180 AND longitude >= -180") \
  #   .where("latitude != 0.000000 OR longitude !=  0.000000 ") \
  #   .write \
  #   .jdbc(url=url, table="mta_reports", mode='append', properties=properties) \
  #   .save()
  # df.write \
  #   .jdbc(url=url, table="mta_reports", mode='append', properties=properties) \
  #   .save()
  # df.show(3)
  df = spark_session.read.format('bigquery')\
    .option('parentProject', 'poetic-standard-368101')\
    .option('table', 'poetic-standard-368101.test_pipeline_elt.postgres__customer__append_only')\
    .load()
  df.show()
  df.write.format('bigquery')\
    .mode('overwrite')\
    .option('credentials', 'ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAicG9ldGljLXN0YW5kYXJkLTM2ODEwMSIsCiAgInByaXZhdGVfa2V5X2lkIjogImUwZjlhZjhhZTYyZTg0OTdjNDE3YjI0OWUwZDBlZjkzZGNmOTlhMTUiLAogICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS1cbk1JSUV2QUlCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktZd2dnU2lBZ0VBQW9JQkFRRGgrK0ZUcFRFbldWd2lcblpaOEhRakpmUWpCa3UrbTNuUGFPQ3FhUlNrSTIrWFlzeGdKeGV6RVhUZ085VUtEZ29oSjBBenNjRjgrOTlWTHBcbm5BWW1oekd4TTRxd3pGcUVLSGRDL0UxVWpsZi9nQXVNUWlKWm16NU16bjRCdklTdG5jWUVqWVBTUW1qRUpKYkRcbjl0Z3RSWTFFS2hsakhhOUJaaTA5eHlDV0YwQnBKcEs4YUozdTVrUGh2RFladitGR2RXZzgzWlJLY3lQbkVMdlRcbnhsNXA1QXcwMmFXbkkyR05zR0o2dDJSM2hvQTN0NS9Wb2lVVVUzMUdKbVFTQkppSlpka013dUtYUnpFVTRoOUpcbi9DU3AvNGJmNVA1K2hySkVOem5MZk85cUFrQUpkS3Q2TEJ1bitKM2RYdXA2MDZFbGI4MnlFd3FRbmZZNmJsS3VcbjdpTDFmMXE1QWdNQkFBRUNnZ0VBRmlNcWk1c2ZHWSt6YXRzeG5QQnJKdEhCRTkwa3BBd0lTeG5YLzF1YTZPKy9cbkY5dTkyWWdMNHhDTlpUcEV5RHlZT2kwbTJUV3I4QnZjSjI1VlcramFnVlZmUExxSUUzSXpYS2lDbXNubTdPeUtcbk5RODFkam15L0tzdHhOWEcyWXNmSHNzT0lzYkdwSkJCQktsbHUrbVlVUmtlcTRyNDVCc3lTMTc2WGptZzRhdnFcbmlmYXZ1YkJUMW9lbHFwb0FPV0VlY0ljN1B3cEVEaUdkRzI4dFMzY24xbHBSNkNZTnlMbkF4STNBMHhKMWdQcEtcbklGcVA1YTBkTEZMR28rakJVRThQQnZXZC94dFNiOHlmMi92KzZMMHVGSnJrRmNXeHZVeXdkWmlaYjlzZkh5MHJcbm03cWprbFNRd0lWa0xqVFpFK2VUT1JTV1VFWHNCM1Z4eEFMWDlMdzJBUUtCZ1FEMUoxaTNrdTJycFREdk9VUWtcbkZodEw1T2xCRVVoUmxYc1JwUnZrZEVYcnU2TWhQbEcyRG9OcFR3UUZyNmpJWituZmRuMXZsYzVoTXRXbjFwYUhcblRRMmxqam1SYnZyN1VYUDZPZi9vbmpHWDB3M3NiSHFLdEFCa3AxQW1odzlVL3dML1lMdU1VblI1SDJzZU1UQ2xcbmViQmZ6alpxL1pCd0haTFVlYis4TnYvTnlRS0JnUURyKzJuS2wwMjFKdDlUakI3TDQwSmFLaHhlZFJSZUw0dFRcbjJhUG9ZMzhOUmUxTHNCNG5VWk9oL2Fubm1hT2k4SUtBb0FoM21SM01iVWE2WHQwSUdnbHZFVUNMMHpzVThPejJcblRlQ3ZWaURyOHMxNUZRSE9jU3lONlpoOG9Hb2JSelBWaUs2aVJxMVUzajdsTDVqTnRGVzI2bWl3bTFabmJ2RWlcbjZqRDBaZmZkY1FLQmdCU2xGcHlHL0FyYlkyNlI1UUlnOW5XQ3RuM3JSYXJSS3NjR24zMnlxUHk4RlhYRU9MOWJcbkk0Zm54by93QmdleWNJaUlBdkkxckdhVkVGY05CQ29xdS81NEpyTHZwS1VyYTlmanNJQlhhaGUrQjBza3E0RW9cbnZXdThoMHhuVFV3OEdYV0dJT1VsUUhlK3hKZUxUdXh3ZmdEamJjTyt6TDRVVDN3ak1KbGFERnE1QW9HQVJxSFpcbjZHbWhPTGVKNE5mdjM5Y0Y1VGIxaHRCRWFHeVNwMlpkU1JGKzFkbzRUSTlLZHpwVGxnajI4ZnRxbnpxZDFTSVJcblpyck4wSUpreVNQYnhMRUdZZGdvR2JILzNTUy95SGxHNXpSQVpsOG1ZVGVJY3VJdEU4ejNkMVlNYkYzRGhnYURcblBzOG8wbDR6TlNZcDltZ2p2N3dwMmJLbENoQ0szSWg4WHFxbXBnRUNnWUFGQ1NNdjJzcCtYS3BLa0RlempsSC9cbmptRkpHZDVkcG1VaE94aysvMG5oY2dnQ2xmaDA5d3ZtUk12ZXJWdWJVNmJwcFFZcFphVVZtMXo1VWkyN3JoSHFcbll3cGNyYmh2MWd6QndUVWFrOFdqNUxPRFkwY1h6dG1uMWg0alVzcE43QW9jRklHR2gwcjhCc25UQlF3Zm9YNmpcbkxIQzYwUWdaanJoK2xqajM3R2hDUlE9PVxuLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLVxuIiwKICAiY2xpZW50X2VtYWlsIjogImdjcC1haXJmbG93QHBvZXRpYy1zdGFuZGFyZC0zNjgxMDEuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJjbGllbnRfaWQiOiAiMTA1NjA1MzQwMDcyMTk1NTk1ODI4IiwKICAiYXV0aF91cmkiOiAiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGgiLAogICJ0b2tlbl91cmkiOiAiaHR0cHM6Ly9vYXV0aDIuZ29vZ2xlYXBpcy5jb20vdG9rZW4iLAogICJhdXRoX3Byb3ZpZGVyX3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vb2F1dGgyL3YxL2NlcnRzIiwKICAiY2xpZW50X3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vcm9ib3QvdjEvbWV0YWRhdGEveDUwOS9nY3AtYWlyZmxvdyU0MHBvZXRpYy1zdGFuZGFyZC0zNjgxMDEuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iCn0=')\
    .option("temporaryGcsBucket","thuong_lake")\
    .option('parentProject', 'poetic-standard-368101')\
    .option('table', 'poetic-standard-368101.test_pipeline_elt.result')\
    .save()
if __name__ == '__main__':
  main()