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


def get_query_results() -> pd.DataFrame:
    df = pd_gbq.read_gbq('''
        select 
            *
        from `bergzeit.dbt_metadata.test_results_latest_errors`
    ''', project_id='bergzeit', dialect='standard')
    
    df = df.astype(str)
    df = df.mask((df == '') | (df == 'None'), None)

    if 'test_result' in df.columns:
        df = df.sort_values(by='test_result', ascending=True)
    else: 
        print('Column "test_result" missing in dataframe. Was the column renamed?')
    
    return df


def evaluate_test_results(df: pd.DataFrame) -> tuple:

    job_run_id = df.iloc[0]['audit_job_id']
    audit_run_url = df.iloc[0]['audit_run_url']
    
    nr_failed_tests = df.shape[0]

    # change dataframe assignment, as ref columns are empty
    source_results = df[df['source_refs'].notnull()][
        ['test_result', 'source_refs', 'column_names', 'test_name', 'test_name_long']
    ]
    source_results['source_refs'] = source_results['source_refs'].str.replace(
        'sources.', '', regex=False
    )

    model_results = df[df['source_refs'].isnull()][
        ['test_result', 'model_refs', 'column_names', 'test_name', 'test_name_long']
    ]
    model_results['model_refs'] = model_results['model_refs'].str.replace(
        'models.', '', regex=False
    )

    return (job_run_id, nr_failed_tests, audit_run_url, source_results, model_results)


def send_teams_message( job_run_id: str,
    nr_failed_tests: str,
    audit_run_url: str,
    source_results: pd.DataFrame,
    model_results: pd.DataFrame
) -> None:

    webhook_url = jobid_webhook_lookup.get(job_run_id).get('webhook')

    teams_message = pymsteams.connectorcard(webhook_url, verify=False)
    teams_message.title("Number of failed tests on Job run id " + job_run_id + ": " + str(nr_failed_tests))

    if source_results.shape[0] > 0:
        section1 = pymsteams.cardsection()
        section1.title("##### **Source tests**")
        source_results = format_test_results(source_results, node_type='source')
        html_table = source_results.to_html(index=False, border=0, escape=False)
        section1.text(html_table)
        teams_message.addSection(section1)
    else:
        print('No source errors or warnings')

    if model_results.shape[0] > 0:
        section2 = pymsteams.cardsection()
        section2.title("##### **Model tests**" )
        model_results = format_test_results(model_results, node_type='model')
        html_table = model_results.to_html(index=False, border=0, escape=False)
        section2.text(html_table)
        teams_message.addSection(section2)
    else: 
        print('No model errors or warnings')
    
    teams_message.addLinkButton("View in dbt" , audit_run_url)
    teams_message.summary("placeholder")
    teams_message.send()

def format_test_results(df: pd.DataFrame, node_type: str) -> pd.DataFrame:
    # Create a URL to dbt explore for each model/source reference
    base_url = f'https://cloud.getdbt.com/explore/{DBT_ACCOUNT_ID}/projects/{DBT_PROJECT_ID}/environments/production/'
    if node_type == 'model':
        dbt_url = base_url + 'details/model.dbt_analytics.'
    if node_type == 'source':
        dbt_url = base_url + 'resource/source/'

    # Format model/source references as links
    df[f'{node_type}_refs'] = df[f'{node_type}_refs'].apply(
        lambda x: f'<a href="{dbt_url}{x}">{x}</a>'
    )

    # Clean 'test_name_long' from package names
    df['test_name_long'] = df['test_name_long'].str.replace(
        '(dbt_expectations|dbt_utils|source)_+', '', regex=True
    )

    # Remove all column_names and test_names from 'test_name_long'
    df['test_name_long'] = df.apply(
        lambda x: re.sub(
            f'({"|".join(map(re.escape, str(x.column_names).split(",") + [x.test_name]))})',
            '',
            x.test_name_long,
        ),
        axis=1,
    )

    # Replace double underscores and format output
    df['test_name_long'] = df.apply(
        lambda x: '<br>'.join(
            re.sub('__+', '__', x.test_name_long.strip('_')).split('__')
        ),
        axis=1,
    )

    return df
