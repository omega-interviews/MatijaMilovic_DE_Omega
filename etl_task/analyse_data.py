import pandas as pd
import plotly.graph_objects as go


def location_chart(dwh_connection):
    query = 'SELECT * FROM dwh.loc_dep_transactions'

    df = pd.read_sql(query, dwh_connection)

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df['location_name'], y=df['transaction_count'], name='Transactions per Location'))

    fig.update_layout(title='Transaction Count Over Store Location',
                      xaxis_title='Location Name',
                      yaxis_title='Transaction Count')

    fig.show()


def date_chart(dwh_connection):
    query = 'SELECT * FROM dwh.date_dep_transactions'

    df = pd.read_sql(query, dwh_connection)

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df['quarter'], y=df['transaction_count'], name='Transactions per Date'))

    fig.update_layout(title='Transaction Count Over Quarter',
                      xaxis_title='Quarter',
                      yaxis_title='Transaction Count')

    fig.show()
