import boto3
import csv
import StringIO
import time


def s3_operate_objects(op, source_bucket, target_bucket='', prefix='', target_prefix='', delimiter='/'):
    """
    s3_operate_objects('cp', services-addons-logs', 'tmp', 'services-logs/EGX44RIFURRUW.2019', 'EGX44RIFURRUW.2019')
    Copies or deletes objects in S3.
    op must be 'copy' or 'delete'.
    target_bucket required if copy.
    """
    if op == 'copy' and not target_bucket:
        return False
    s3 = boto3.client('s3')
    continuing = True
    token=''
    while continuing:
        if token:
            r = s3.list_objects_v2(Bucket=source_bucket, Prefix=prefix, Delimiter=delimiter, ContinuationToken=token)
        else:
            r = s3.list_objects_v2(Bucket=source_bucket, Prefix=prefix, Delimiter=delimiter)
        if 'NextContinuationToken' in r:
            token = r['NextContinuationToken']
            continuing = True
        else:
            continuing = False
        if 'Contents' in r:
            for obj in r['Contents']:
                source_key = {'Bucket': source_bucket, 'Key': obj['Key']}
                target_key = obj['Key']
                # If we set a new target prefix, then replace the old one with it.
                if target_prefix:
                    target_key = obj['Key'].replace(prefix, target_prefix)
                if op == 'copy':
                    s3.copy_object(CopySource=source_key, Bucket=target_bucket, Key=target_key)
                elif op == 'delete':
                    s3.delete_object(Bucket=source_bucket, Key=obj['Key'])


def s3_copy_objects(source_bucket, target_bucket='', prefix='', target_prefix='', delimiter='/'):
    s3_operate_objects('copy', source_bucket, target_bucket, prefix, target_prefix, delimiter)


# Prefix required to prevent accidental deletion of entire bucket.
def s3_delete_objects(source_bucket, prefix, delimiter='/'):
    s3_operate_objects('delete', source_bucket, prefix=prefix, delimiter=delimiter)


class Athena(object):
    """
    Class to create AWS Athena queries and read the results.
    """
    def __init__(self, database, data_location, result_bucket, timeout=300):
        self.database = database
        self.result_bucket = result_bucket
        self.data_location = data_location
        self.client = boto3.client('athena')
        self.timeout = timeout
        self.table_name = ''

    def check_queries_state(self, query_ids):
        response = self.client.batch_get_query_execution(QueryExecutionIds=query_ids)
        statuses = [r['Status']['State'] for r in response['QueryExecutions']]
        return statuses

    # query_ids is a list of execution ids
    def wait_for_query(self, query_ids):
        end_states = ['SUCCEEDED', 'FAILED', 'CANCELLED']
        statuses = None
        wait = 0
        timeout = self.timeout
        while (wait < timeout):
            statuses = self.check_queries_state(query_ids)
            # Check if any queries are not in end state.
            if all(s in end_states for s in statuses):
                # Stop waiting because all queries are in end state.
                break
            else:
                time.sleep(5)
                wait = wait + 5
        if wait == timeout:
            print "Timed out waiting for queries, check Athena query history."
            return None

        return statuses

    def start_query(self, query):
        output = 's3://' + self.result_bucket
        response = self.client.start_query_execution(
                   QueryString=query,
                   QueryExecutionContext={'Database': self.database},
                   ResultConfiguration={'OutputLocation': output,}
                )
        return response

    def get_query_result(self, qid):
        return self.client.get_query_results(QueryExecutionId=qid)

    def create_table(self, query, name):
        table_query = query.format(name, self.data_location)
        state = self.start_query(table_query)
        response = self.wait_for_query([state['QueryExecutionId']])
        if response:
            self.table_name = name
            return name
        else:
            return False

    def drop_table(self):
        if self.table_name:
            query = "DROP TABLE IF EXISTS {0}".format(self.table_name)
            self.start_query(query)
        else:
            raise ValueError("No created table to drop.")

    def cleanup(self):
        print "Cleaning up temporary files and tables..."
        if self.table_name:
            self.drop_table()

    def construct_json(self, csv):
        total = 0
        data = {}
        data["versions"] = {}

        for row in csv:
            data["versions"][row[1]] = int(row[2])
            total += int(row[2])

        data["count"] = total
        return data


class AthenaLogs(Athena):
    def __init__(self, database, data_bucket, result_bucket, log_prefix, timeout=300):
        super(AthenaLogs, self).__init__(database, data_bucket, result_bucket, timeout)
        self.data_bucket = data_bucket
        self.log_prefix = log_prefix
        self.temp_prefix = 'tmp/' + log_prefix
        s3_copy_objects(self.data_bucket, self.data_bucket, log_prefix, self.temp_prefix)
        self.data_location = self.data_bucket + '/' + self.temp_prefix.rsplit('/', 1)[0] + '/'

    def cleanup(self):
        super(AthenaLogs, self).cleanup()
        s3_delete_objects(self.data_bucket, self.temp_prefix)

