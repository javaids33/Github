import boto3
import pandas as pd
import json
import subprocess
from collections import Counter
from sql_metadata import Parser

# Configuration
S3_BUCKET = 'your-trino-query-logs-bucket'
LOG_FILE_KEY = 'query.log'
TABLE_NAME = 'my_catalog.my_schema.my_table'
HIGH_CARDINALITY_THRESHOLD = 100000
LOW_CARDINALITY_THRESHOLD = 1000

# Initialize S3 client
s3 = boto3.client('s3')

def download_logs(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return response['Body'].read().decode('utf-8')

def parse_logs(log_content):
    logs = []
    for line in log_content.strip().split('\n'):
        try:
            log_entry = json.loads(line)
            logs.append(log_entry)
        except json.JSONDecodeError:
            continue
    return pd.DataFrame(logs)

def extract_columns_metadata(sql):
    parser = Parser(sql)
    return {
        'select': parser.columns_dict.get('select', []),
        'where': parser.columns_dict.get('where', []),
        'join': parser.columns_dict.get('joins', []),
        'group_by': parser.columns_dict.get('groupby', []),
        'order_by': parser.columns_dict.get('orderby', [])
    }

def get_column_cardinality(table, column):
    query = f"SELECT COUNT(DISTINCT {column}) FROM {table};"
    result = subprocess.run(['trino', '--execute', query], capture_output=True, text=True)
    if result.returncode == 0:
        try:
            count = int(result.stdout.strip().split('\n')[-1])
            return count
        except:
            return None
    else:
        print(f"Error fetching cardinality for {column}: {result.stderr}")
        return None

def generate_partition_spec(partition_columns):
    spec = []
    for pc in partition_columns:
        if pc['strategy'] == 'hash':
            spec.append(f"HASH({pc['column']}, 16)")
        elif pc['strategy'] == 'range':
            spec.append(f"{pc['column']}")
    return ",".join(spec)

def alter_table_partitioning(table, partition_spec):
    query = f"ALTER TABLE {table} SET PROPERTIES ('partitioning' = '{partition_spec}');"
    result = subprocess.run(['trino', '--execute', query], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Successfully altered partitioning for {table}")
    else:
        print(f"Error altering table: {result.stderr}")

def optimize_table(table):
    query = f"CALL system.optimize('{table}');"
    result = subprocess.run(['trino', '--execute', query], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Optimization initiated for {table}")
    else:
        print(f"Error optimizing table: {result.stderr}")

def main():
    # Step 1: Download and parse logs
    log_content = download_logs(S3_BUCKET, LOG_FILE_KEY)
    df_logs = parse_logs(log_content)
    
    # Step 2: Extract columns
    df_logs['parsed_columns'] = df_logs['sql'].apply(extract_columns_metadata)
    
    # Step 3: Aggregate usage
    select_counter = Counter()
    where_counter = Counter()
    join_counter = Counter()
    group_by_counter = Counter()
    order_by_counter = Counter()
    
    for index, row in df_logs.iterrows():
        cols = row['parsed_columns']
        select_counter.update(cols['select'])
        where_counter.update(cols['where'])
        join_counter.update(cols['join'])
        group_by_counter.update(cols['group_by'])
        order_by_counter.update(cols['order_by'])
    
    usage_stats = pd.DataFrame({
        'select': select_counter,
        'where': where_counter,
        'join': join_counter,
        'group_by': group_by_counter,
        'order_by': order_by_counter
    }).fillna(0).astype(int).reset_index().rename(columns={'index': 'column'})
    
    # Step 4: Fetch cardinality
    columns_to_check = usage_stats['column'].tolist()
    cardinality = {}
    for column in columns_to_check:
        count = get_column_cardinality(TABLE_NAME, column)
        if count:
            cardinality[column] = count
    cardinality_df = pd.DataFrame(list(cardinality.items()), columns=['column', 'cardinality'])
    
    # Step 5: Merge stats
    merged_stats = usage_stats.merge(cardinality_df, on='column', how='left')
    
    # Step 6: Determine partition columns
    partition_columns = []
    for _, row in merged_stats.iterrows():
        usage_count = row['where'] + row['join'] + row['group_by'] + row['order_by']
        if usage_count > 50:  # Example usage threshold
            if row['cardinality'] > HIGH_CARDINALITY_THRESHOLD:
                partition_columns.append({'column': row['column'], 'strategy': 'hash'})
            elif row['cardinality'] < LOW_CARDINALITY_THRESHOLD:
                partition_columns.append({'column': row['column'], 'strategy': 'range'})
            else:
                partition_columns.append({'column': row['column'], 'strategy': 'range'})
    
    # Step 7: Generate partition spec
    partition_spec = generate_partition_spec(partition_columns)
    print(f"Recommended Partition Spec: {partition_spec}")
    
    # Step 8: Alter table and optimize
    if partition_spec:
        alter_table_partitioning(TABLE_NAME, partition_spec)
        optimize_table(TABLE_NAME)
    else:
        print("No suitable partition columns identified.")

if __name__ == "__main__":
    main()
