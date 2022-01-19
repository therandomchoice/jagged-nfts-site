import dash
from dash import dcc, html
from dash import Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import redis
import logging
import pickle
import sys
import os

logging.basicConfig(stream=sys.stdout, format='%(levelname)s %(message)s', level=logging.INFO)

redis_url = os.getenv('REDISTOGO_URL', 'redis://localhost:6379')
redis_conn = redis.from_url(redis_url)

app = dash.Dash('jagged-nfts', title='jagged nfts',
                external_stylesheets=[dbc.themes.SLATE])
server = app.server



info = html.Div(className='my-3', children=[
    html.Ul([
        html.Li([
            'Open blockchain data obtained from the ',
            html.A('etherscan.io', href='https://etherscan.io/'),
            ' is used. ',
            'Smart contract transactions associated with the Foundation are selected for analysis. ',
            'Transactions are divided into three types.',
            html.Ul([
                html.Li('Bids made in auctions.'),
                html.Li(
                    '''Outbids - bets that were outbid by other participants.
                    In this case, the previously made bet was returned to jagged,
                    so these values are taken with a negative sign.
                    '''),
                html.Li('Accepted private auctions. '),
            ]),
        ]),
        html.Li('Values in USD are tied to the current ether exchange rate.'),
        html.Li(
            '''This site only has a dark theme.
            This is because jagged spent more on NFT than I have earned in my entire life.
            OMG! WTF? lol...
            '''),
        html.Li([
            'It is an open source project. Code is available on ',
            html.A('GitHub', href='https://github.com/therandomchoice/jagged-nfts-site'), '.',
        ]),
    ])
])

main_table = dbc.Table(bordered=True, children=[
    html.Tbody([
        html.Tr([
            html.Td(html.H3('total spent in ether')),
            html.Td(html.H3(id='total-eth')),
        ]),
        html.Tr([
            html.Td(html.H3('total spent in USD')),
            html.Td(html.H3(id='total-usd')),
        ]),
    ]),
])

graph_config = html.Div([
    dbc.Label('Aggregate by', class_name='pe-3'),
    dbc.RadioItems(
        id='freq', value='D', inline=True, class_name='d-inline-block',
        options=[
            dict(label='hour', value='H'), dict(label='day', value='D'),
            dict(label='week', value='W'), dict(label='month', value='M'),
            dict(label='year', value='Y')
    ]),
    dbc.Checklist(id='bymethod', value=[], options=[
        dict(label='Split into bids, outbids and private auctions ', value='do')
    ]),
    dbc.Label('Show in', class_name='pe-3'),
    dbc.RadioItems(
        id='currency', value='eth', inline=True, class_name='d-inline-block',
        options=[
            dict(label='ether', value='eth'), dict(label='USD', value='usd')
    ]),
])

app.layout = dbc.Container(children=[
    html.H1(className='p-3', children=[
        'This site is dedicated to a single purpose - to show how much ',
        html.A('jagged', href='https://foundation.app/@jagged'),
        ' spends to buy NFTs on the ',
        html.A('Foundation', href='https://foundation.app'),
        '.',
    ]),
    html.Div(className='my-3', children=[
        dbc.Button('Show additional info', id='show-info'),
        dbc.Collapse(info, id='info', is_open=False)
    ]),
    html.Div(className='m-5', children=[
        main_table,
    ]),
    dcc.Graph(id='graph'),
    graph_config,
])



@app.callback(
    Output('info', 'is_open'),
    Output('show-info', 'children'),
    Input('show-info', 'n_clicks'),
    State('info', 'is_open'))
def toggle_info(n, is_open):
    if n:
        is_open = not is_open
    text = ('Hide' if is_open else 'Show') + ' additional info'
    return is_open, text



@app.callback(
    Output('graph', 'figure'),
    Output('total-eth', 'children'),
    Output('total-usd', 'children'),
    Input('freq', 'value'),
    Input('bymethod', 'value'),
    Input('currency', 'value'))
def update_graph(freq, bymethod, currency):
    try:
        df = pd.DataFrame(pickle.loads(redis_conn.get('summary')))
    except:
        df = pd.read_csv('preloaded.csv', parse_dates=['timeStamp'])
        logging.warning('No redis summary found, load preloaded.csv')

    by = pd.Grouper(key='timeStamp', freq=freq)
    fig_color = None
    if 'do' in bymethod:
        by = ['method', by]
        fig_color = 'type'
    agg = df.groupby(by=by).agg({'value': ['sum', 'count'], 'usd': 'sum'}).reset_index()
    agg.columns = [''.join(col) for col in agg.columns]
    if 'method' in agg.columns:
        agg.method = agg.method.map(dict(
            bid='bids', outbid='outbids', private_auction='private auctions'))
    agg.valuesum, agg.usdsum = agg.valuesum.round(2), agg.usdsum.round()
    agg = agg.rename(columns=dict(
        timeStamp='time', valuesum='ether spent', usdsum='USD spent',
        method='type', valuecount='transactions'))

    yval = {'eth': 'ether spent', 'usd': 'USD spent'}[currency]

    fig = px.bar(agg, x='time', y=yval, color=fig_color, template='plotly_dark',
                 hover_data=['time', 'ether spent', 'USD spent', 'transactions'])
    fig.update_layout(
        legend=dict(x=.02, y=.98, xanchor='left', yanchor='top', title=None),
        margin=dict(t=30, b=10, r=10, l=10), xaxis=dict(title=None), yaxis=dict(title=None))

    total_eth = f'{df.value.sum():,.2f} eth'.replace(',', ' ')
    total_usd = f'$ {df.usd.sum():,.0f}'.replace(',', ' ')
    return fig, total_eth, total_usd



if __name__ == '__main__':
    app.run_server(debug=True)