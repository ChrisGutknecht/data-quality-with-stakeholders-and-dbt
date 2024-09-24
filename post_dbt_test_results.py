import requests
import pandas as pd
from google.cloud import storage
import pymsteams

jobid_webhook_lookup = {
    # specific job ids map to different teams chanel
    '123': { 
        'job_name': 'test_models_channel_1',
        'webhook': 'https://youe.webhook.office.com/webhookb2/...'
    },
    '124': { 
        'job_name': 'test_models_channel_2',
        'webhook': 'https://youe.webhook.office.com/webhookb2/...'
    }
}

def get_failed_tests(data, context):
    failed_tests = get_query_results()

    if len(failed_tests) > 0:
        job_run_id, nr_failed_tests, audit_run_url, source_results, model_results = evaluate_test_results(failed_tests)
        send_teams_message(job_run_id, nr_failed_tests, audit_run_url, source_results, model_results)

    return('Failed tests transferred to Teams', '200')


def get_query_results():
    df = pd.io.gbq.read_gbq('''
        select 
            *
        from `your_project.dbt_metadata.test_results_latest_errors`
    ''', project_id='your_project', dialect='standard')
    
    df = df.astype(str)
    df = df.mask(df == '', None)

    if 'test_result' in df.columns:
        df = df.sort_values(by='test_result', ascending=True)
    else: 
        print('Column "test_result" missing in dataframe. Was the column renamed?')
    
    return df


def evaluate_test_results(df):

    job_run_id = df.iloc[0]['audit_job_id']
    audit_run_url = df.iloc[0]['audit_run_url']
    
    nr_failed_tests = df.shape[0]

    # change dataframe assignment, as ref columns are empty
    source_results = df[df['test_name_long'].str.contains('_source_', na=False)][['test_result', 'source_refs', 'column_names', 'test_name', 'test_name_long']]
    source_results['source_refs'] = source_results['source_refs'].str.replace('sources.', '', regex=False)

    model_results = df[~df['test_name_long'].str.contains('_source_', na=False)][['test_result', 'model_refs', 'column_names', 'test_name', 'test_name_long']] 
    model_results['model_refs'] = model_results['model_refs'].str.replace('models.', '', regex=False)

    return job_run_id, nr_failed_tests, audit_run_url, source_results, model_results


def send_teams_message(job_run_id, nr_failed_tests, audit_run_url, source_results, model_results):

    webhook_url = jobid_webhook_lookup.get(job_run_id).get('webhook')

    teams_message = pymsteams.connectorcard(webhook_url, verify=False)
    teams_message.title("Number of failed tests on Job run id " + job_run_id + ": " + str(nr_failed_tests))

    if source_results.shape[0] > 0:
        section1 = pymsteams.cardsection()
        section1.title("##### **Source tests**")
        html_table = source_results.to_html(index=False, border=0)
        section1.text(html_table)
        teams_message.addSection(section1)
    else:
        print('No source errors or warnings')

    if model_results.shape[0] > 0:
        section2 = pymsteams.cardsection()
        section2.title("##### **Model tests**" )
        html_table = model_results.to_html(index=False, border=0)
        section2.text(html_table)
        teams_message.addSection(section2)
    else: 
        print('No model errors or warnings')
    
    teams_message.addLinkButton("View in dbt" , audit_run_url)
    teams_message.summary("placeholder")
    teams_message.send()
