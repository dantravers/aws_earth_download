import json
import datetime
import pandas as pd

def fetch_messages(queue_url, client, variables):
    """ Function to extract messages from sqs
    
    Notes
    -----
Takes inputs of the queue url and the variables whose files we want to extract.
    The forecast reference times are hardcoded to be all from 0300 inclusive to 1500 (not inclusive).
    The times forecast are hardcoded to be from hour 7 (inclusive) to 19(not inclusive).
    
    Parameters
    ----------
    queue_url : str
        string of the sqs queue url.
    client : obj:boto3.client 
        boto3 client object initialized with security access keys and region.
    variables : list
        List of the variables names to extract files for.
    """

    mdf = pd.DataFrame([])

    while True:
        resp = client.receive_message(
        QueueUrl= queue_url,
        MaxNumberOfMessages=10)
        try:
            messages = resp['Messages']
        except KeyError:
            print('No messages on the queue!')
            messages = []
            
        # store messages we want:
        for mes in resp['Messages']:
            obj = json.loads(json.loads(mes['Body'])['Message'])
            if (obj['name'] in variables) & \
            (datetime.datetime.strptime(obj['forecast_reference_time'], "%Y-%m-%dT%H:%M:%SZ").hour in range(3, 15)) & \
            (datetime.datetime.strptime(obj['time'], "%Y-%m-%dT%H:%M:%SZ").hour in range(7, 19)):
                print(obj['name'], obj['forecast_reference_time'], obj['time'], int(obj['forecast_period'])/3600/24)
                mdf = mdf.append(pd.DataFrame(obj, index=[mes['MessageId']]))

        #delete messages in queue which are read
        entries = [
        {'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']}
        for msg in resp['Messages']
        ]
        resp = client.delete_message_batch(QueueUrl=queue_url, Entries=entries)
        if len(resp['Successful']) != len(entries):
            raise RuntimeError(
                f"Failed to delete messages: entries={entries!r} resp={resp!r}"
            )
    return(mdf)