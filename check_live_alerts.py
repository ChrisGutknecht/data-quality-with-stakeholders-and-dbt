
import requests
import pandas_gbq
import json
import os
import pymsteams

# Webhook for channel 'Alerts__Prod'
wh_live = '$$ add your prod web hook here$$'
# Webhook for channel 'Alerts_zz_test'
wh_test = '$$ add your dev web hook here$$'

webhook = wh_live

''' Example request payload
 {
    "time_frame_min" : 120,
    "max_event_threshold" : 90,
    "table_name" : "alerts_ga4_pageviews_404_live_overall",
    "message_title_prefix" : "GA4 LiveAlerts | 404 Page View Alert in Overall for last ",
    "message_text_prefix" : "High 404 Events! More details at https://lookerstudio.google.com/s/xyz |  Event count found : "
} 
'''

def check_live_events(request):

    # Parsing the request object for the upload parameters
    request = request.get_data()
    try: 
        request_json = json.loads(request.decode())
        print(request_json)
    except ValueError as e:
        print(f"Error decoding JSON: {e}")
        return ("JSON Error", 400)

    time_frame_min = request_json.get('time_frame_min')
    min_event_threshold = request_json.get('min_event_threshold')
    max_event_threshold = request_json.get('max_event_threshold')
    message_title = request_json.get('message_title_prefix')
    message_text = request_json.get('message_text_prefix')
    table_name = request_json.get('table_name')

    # Preparing the query
    full_table_name = 'your_project.dbt_alerts.' + table_name
    query = 'select * from ' + full_table_name
    
    # Executing the query
    event_count = get_query_results(query)
    print('# of events' + str(event_count))
    event_threshold = min_event_threshold if min_event_threshold else max_event_threshold

    # Max 404 page view check
    if max_event_threshold:
        if event_count > max_event_threshold:
            send_simple_message(event_count, message_title, time_frame_min, message_text, event_threshold)

    # Min event check
    elif event_count < min_event_threshold: 
        send_simple_message(event_count, message_title, time_frame_min, message_text, event_threshold)

    return('Events in last ' + str(time_frame_min) + ' minutes : ' + str(event_count), '200')


def get_query_results(query):
    df = pandas_gbq.read_gbq(query, project_id='bergzeit') 
    return df.iloc[0,0]


def send_simple_message(count, message_title, time_frame_min, message_text, event_threshold):
    teams_message = pymsteams.connectorcard(webhook, verify=False)
    teams_message.title(message_title + str(time_frame_min) + ' minutes')
    teams_message.text(message_text + str(count) + '. Event threshold: ' + str(event_threshold))
    teams_message.send()
