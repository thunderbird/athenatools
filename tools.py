import boto3
import csv
import StringIO
import time

from subprocess import check_output

class Athena(object):
    """
    Class to create AWS Athena queries and read the results.
    """
    def __init__(self, database, data_bucket, result_bucket, timeout=300):
        self.database = database
        self.result_bucket = result_bucket
        self.data_bucket = data_bucket
        self.temp_bucket = result_bucket + '/tmp/'
        self.client = boto3.client('athena')
        self.timeout = timeout

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
        table_query = query.format(name, self.data_bucket)
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
        # s3_cleanup_cmd = 'aws s3 rm s3://{0} --recursive'.format(self.temp_bucket)
        # cleanup_debug = check_output(s3_cleanup_cmd, shell=True)
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

    # Takes a list of {querytype: queryexecutionid} and copies files in s3.
    def copy_results_s3(self, qid, filedir, filename):
        result_filename = filename + '.json'
        result_dir = filedir
        s3 = boto3.resource('s3')
        output_key = '/'.join([result_dir, result_filename])
        rows = []
        input_key = qid + '.csv'

        infile = s3.Object(self.result_bucket,input_key)
        data = infile.get()['Body'].read().splitlines(True)[1:]
        outfile = StringIO.StringIO()
        inreader = csv.reader(data)
        jsondata = construct_json(inreader)
        json.dump(jsondata, outfile)
        outfile.seek(0)
        s3.Object(self.data_bucket, output_key).put(Body=outfile)
        infile.delete()
        return True
