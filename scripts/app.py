import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html, ctx, dash_table
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from dateutil.relativedelta import relativedelta

def render_chart_tab():
    return html.Div([

        html.Div([
            html.Div(
                [
                    html.Button("◀", id="btn-prev-stock", n_clicks=0, className="stock-nav-btn"),
                    dcc.Dropdown(
                        id="stock-dropdown",
                        options=stock_options,
                        value="2330",
                        clearable=False,
                        style={"width": "360px"}
                    ),
                    html.Button("▶", id="btn-next-stock", n_clicks=0, className="stock-nav-btn"),
                ],
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "center",
                    "gap": "12px",
                    "marginTop": "50px",
                    "marginBottom": "10px",
                    "marginLeft": "70px",
                }
            ),

            html.Div([
                html.Button("最近一個月", id="btn-1m", n_clicks=0, style={'margin': '5px'}, className="time-btn"),
                html.Button("最近三個月", id="btn-3m", n_clicks=0, style={'margin': '5px'}, className="time-btn"),
                html.Button("最近六個月", id="btn-6m", n_clicks=0, style={'margin': '5px'}, className="time-btn"),
                html.Button("最近一年", id="btn-1y", n_clicks=0, style={'margin': '5px'}, className="time-btn"),
                html.Button("全部資料", id="btn-all", n_clicks=0, style={'margin': '5px'}, className="time-btn"),
            ], style={
                'textAlign': 'right',
                'marginTop': "50px",
                "marginBottom": "10px",
                "marginLeft": "50px",
            }),
        ],
        style={
            "display": "flex",
            "alignItems": "center",
            "justifyContent": "space-between",
            "width": "90%",
            "margin": "20px 0 0 50px",
        }),

        html.Div([
            dcc.RangeSlider(
                id='date-slider-top',
                updatemode='mouseup',
                min=0,
                max=1,
                value=[0, 1],
                tooltip={
                    "always_visible": False,          
                    "placement": "top",               
                    "transform": "transform_to_date"  
                }
            )
        ], style={'width': '90%', 'margin': 'auto', 'padding': '20px'}),

        dcc.Graph(
            id='stock-charts',
            config={
                'displayModeBar': False,
                'scrollZoom': True,
            }
        ),

        html.Div([
            dcc.RangeSlider(
                id='date-slider-bottom',
                updatemode='mouseup',
                min=0,
                max=1,
                value=[0, 1],
                tooltip={
                    "always_visible": False,          
                    "placement": "top",               
                    "transform": "transform_to_date"  
                }
            )
        ], style={
            'width': '90%',
            'margin': 'auto',
            'padding': '20px'
        }),

    ])

def render_summary_tab():

    return html.Div(

        className="summary-table-wrapper",
        children=[

            html.Div(
                html.Button(
                    "Reset",
                    id="summary-reset-btn",
                    n_clicks=0,
                    className="reset-btn"
                ),
                id="summary-reset-wrapper",
                style={"display": "none", "textAlign": "right", "marginBottom": "6px"}
            ),

            dash_table.DataTable(
                id="summary-table",
                data=summary_df.to_dict("records"),
                columns=[
                    {"name": "代碼", "id": "stock_id"},
                    {"name": "名稱", "id": "stock_name"},
                    {"name": "收盤價", "id": "close"},
                    {"name": "日變動 %", "id": "close_change_pct"},
                    {"name": "3日漲跌 %", "id": "close_3d_change_pct"},
                    {"name": "交易量(張)", "id": "volume"},
                    {"name": "量比 (5D)", "id": "volume_ratio_5d"},
                    {"name": "訊號", "id": "signal_today"},
                    {"name": "距進場(日)", "id": "bars_since_entry"},
                    {"name": "K", "id": "K"},
                    {"name": "DIF", "id": "DIF"},
                ],
                sort_action="native",
                sort_by=[],
                filter_action="native",
                filter_query="",
                page_action="native",
                page_current=0,
                page_size=20,
                cell_selectable=True,
                style_table={"overflowX": "auto"},
                style_cell={
                    "textAlign": "center",
                    "fontFamily": "Arial",
                    "fontSize": "13px",
                    "padding": "6px",
                },
                style_header={
                    "backgroundColor": "#F0F0F0",
                    "fontWeight": "bold",
                },
                style_data_conditional=[
                    {
                        "if": {"filter_query": "{signal_today} != 'none'"},
                        "backgroundColor": "#FFF4E5",
                    },
                    {
                        "if": {"filter_query": "{close_change_pct} > 0"},
                        "color": "red",
                    },
                    {
                        "if": {"filter_query": "{close_change_pct} < 0"},
                        "color": "green",
                    },
                ],
            )  
        ]
    )

# load data
df = pd.read_parquet('data/processed/daily.parquet')
df = df.sort_values(["stock_id", "date"])

summary_df = pd.read_parquet('data/processed/summary.parquet')

# Dash app
app = dash.Dash(__name__, suppress_callback_exceptions=True)

# define stock options
stock_options = []
unique_stocks = df[['stock_id', 'stock_name']].drop_duplicates()
for _, row in unique_stocks.iterrows():
    stock_options.append({'label': f"{row['stock_id']} - {row['stock_name']}", 'value': row['stock_id']})

stock_id_list = [opt["value"] for opt in stock_options]

app.layout = html.Div([

    dcc.Store(
        id="summary-clicked-stock",
        storage_type="memory"
    ),

    html.Div([
        html.H1("Dashboard", style={'textAlign': 'center', 'margin': '0', 'padding': '20px 0'}),

        dcc.Tabs(
            id="tabs",
            value="tab-table",
            parent_className="custom-tabs-container",
            className="custom-tabs",
            children=[
                dcc.Tab(
                    label="Summary Table",
                    value="tab-table",
                    className="custom-tab",
                    selected_className="custom-tab--selected"
                ),
                dcc.Tab(
                    label="Chart",
                    value="tab-chart",
                    className="custom-tab",
                    selected_className="custom-tab--selected"
                ),
            ],
            style={
                'height': '44px',
                'display': 'flex',
                'justifyContent': 'flex-start'
            }
        ),

    ], className="top-section"),

    html.Div(
        id="tab-content",
        children=[
            html.Div(render_summary_tab(), id="tab-table-div", style={"display": "none"}),
            html.Div(render_chart_tab(), id="tab-chart-div", style={"display": "block"}),
        ]
    )
])

@app.callback(
    [
        Output("tab-table-div", "style"),
        Output("tab-chart-div", "style"),
    ],
    Input("tabs", "value"),
)
def switch_tab(tab):
    if tab == "tab-table":
        return {"display": "block"}, {"display": "none"}
    return {"display": "none"}, {"display": "block"}

@app.callback(
    Output("summary-reset-wrapper", "style"),
    [
        Input("summary-table", "filter_query"),
        Input("summary-table", "sort_by"),
    ]
)
def toggle_reset_button(filter_query, sort_by):
    has_filter = filter_query is not None and filter_query != ""
    has_sort = sort_by is not None and len(sort_by) > 0

    if has_filter or has_sort:
        return {"display": "block", "textAlign": "right", "marginBottom": "6px"}

    return {"display": "none"}

@app.callback(
    [
        Output("summary-table", "filter_query"),
        Output("summary-table", "sort_by"),
        Output("summary-table", "page_current"),
    ],
    Input("summary-reset-btn", "n_clicks"),
    prevent_initial_call=True
)
def reset_summary_table(n):
    return "", [], 0


@app.callback(
    [Output('date-slider-top', 'min'), Output('date-slider-top', 'max'), Output('date-slider-top', 'value'), Output('date-slider-top', 'marks'),
     Output('date-slider-bottom', 'min'), Output('date-slider-bottom', 'max'), Output('date-slider-bottom', 'value'), Output('date-slider-bottom', 'marks')],
    [Input('stock-dropdown', 'value')],
    [dash.dependencies.State('date-slider-top', 'value')]
)
def update_slider_range(selected_stock, current_range):
    stock_data = df[df['stock_id'] == selected_stock]
    dates = pd.to_datetime(stock_data['date']).sort_values()

    min_ts = int(dates.min().timestamp())
    max_ts = int(dates.max().timestamp())

    if current_range and isinstance(current_range, list):
        new_start = max(min_ts, current_range[0])
        new_end = min(max_ts, current_range[1])
        
        if new_start < new_end:
            target_value = [new_start, new_end]
        else:
            target_value = [min_ts, max_ts]
    else:
        target_value = [min_ts, max_ts]
    
    mark_dates = pd.date_range(start=dates.min(), end=dates.max(), periods=15)
    marks = {
        int(d.timestamp()): {
            'label': d.strftime('%Y/%m'),
            'style': {
                'fontSize': '12px',
                'fontFamily': 'Arial'
                }
        } for d in mark_dates
    }
    
    return min_ts, max_ts, target_value, marks, min_ts, max_ts, target_value, marks

@app.callback(
    Output("stock-dropdown", "value"),
    [
        Input("btn-prev-stock", "n_clicks"),
        Input("btn-next-stock", "n_clicks"),
    ],
    State("stock-dropdown", "value"),
    prevent_initial_call=True
)
def switch_stock(prev_clicks, next_clicks, current_stock):
    if current_stock not in stock_id_list:
        raise PreventUpdate

    current_idx = stock_id_list.index(current_stock)
    trigger = ctx.triggered_id

    if trigger == "btn-prev-stock":
        new_idx = max(current_idx - 1, 0)
    elif trigger == "btn-next-stock":
        new_idx = min(current_idx + 1, len(stock_id_list) - 1)
    else:
        raise PreventUpdate

    return stock_id_list[new_idx]

# link top and bottom slider
@app.callback(
    Output('date-slider-top', 'value', allow_duplicate=True),
    Input('date-slider-bottom', 'value'),
    prevent_initial_call=True
)
def sync_top_slider(bottom_val):
    return bottom_val

@app.callback(
    Output('date-slider-bottom', 'value', allow_duplicate=True),
    Input('date-slider-top', 'value'),
    prevent_initial_call=True
)
def sync_bottom_slider(top_val):
    return top_val

@app.callback(
    Output('date-slider-top', 'value', allow_duplicate=True),
    [Input('btn-1m', 'n_clicks'),
     Input('btn-3m', 'n_clicks'),
     Input('btn-6m', 'n_clicks'),
     Input('btn-1y', 'n_clicks'),
     Input('btn-all', 'n_clicks')],
    [State('stock-dropdown', 'value')],
    prevent_initial_call=True
)
def update_range_by_button(n1, n3, n6, n1y, nall, selected_stock):
    button_id = ctx.triggered_id
    if not button_id:
        raise PreventUpdate

    stock_data = df[df['stock_id'] == selected_stock]
    max_date = pd.to_datetime(stock_data['date'].max())
    min_date_limit = pd.to_datetime(stock_data['date'].min())

    if button_id == "btn-1m":
        start_date = max_date - relativedelta(months=1)
    elif button_id == "btn-3m":
        start_date = max_date - relativedelta(months=3)
    elif button_id == "btn-6m":
        start_date = max_date - relativedelta(months=6)
    elif button_id == "btn-1y":
        start_date = max_date - relativedelta(years=1)
    else:
        start_date = min_date_limit

    start_date = max(start_date, min_date_limit)
    
    return [int(start_date.timestamp()), int(max_date.timestamp())]

@app.callback(
    [Output('date-slider-top', 'value', allow_duplicate=True),
     Output('date-slider-bottom', 'value', allow_duplicate=True)],
    Input('stock-charts', 'relayoutData'),
    prevent_initial_call=True
)
def update_sliders_from_chart(relayoutData):
    if relayoutData and ('xaxis.range[0]' in relayoutData or 'xaxis3.range[0]' in relayoutData):
        try:
            start_str = relayoutData.get('xaxis.range[0]') or relayoutData.get('xaxis3.range[0]')
            end_str = relayoutData.get('xaxis.range[1]') or relayoutData.get('xaxis3.range[1]')
            
            start_ts = int(pd.to_datetime(start_str).timestamp())
            end_ts = int(pd.to_datetime(end_str).timestamp())
            
            return [start_ts, end_ts], [start_ts, end_ts]
        except Exception:
            raise PreventUpdate
            
    raise PreventUpdate

@app.callback(
    [
        Output("summary-clicked-stock", "data"),
        Output("tabs", "value"),
    ],
    Input("summary-table", "active_cell"),
    State("summary-table", "derived_viewport_data"),
    prevent_initial_call=True
)
def jump_to_chart(active_cell, table_data):
    if not active_cell:
        raise PreventUpdate
    
    if active_cell["column_id"] not in ["stock_id", "stock_name"]:
        raise PreventUpdate

    row = active_cell["row"]
    stock_id = table_data[row]["stock_id"]

    return stock_id, "tab-chart"

@app.callback(
    Output("stock-dropdown", "value", allow_duplicate=True),
    Input("summary-clicked-stock", "data"),
    prevent_initial_call=True
)
def update_stock_from_summary(stock_id):
    if not stock_id:
        raise PreventUpdate
    return stock_id


@app.callback(
    Output('stock-charts', 'figure'),
    [Input('stock-dropdown', 'value'),
     Input('date-slider-top', 'value')]
)
def update_charts(selected_stock, date_range):

    if not date_range or not isinstance(date_range, list) or len(date_range) < 2:
        raise PreventUpdate
    
    full_stock_data = df[df['stock_id'] == selected_stock].sort_values('date').copy()
    if full_stock_data.empty:
        return go.Figure()
            
    start_dt = pd.to_datetime(date_range[0], unit='s')
    end_dt = pd.to_datetime(date_range[1], unit='s')

    display_data = full_stock_data[(full_stock_data['date'] >= start_dt) & (full_stock_data['date'] <= end_dt)].copy()

    if display_data.empty:
        return go.Figure()
    
    # set up subplots
    fig = make_subplots(
        rows=3, cols=1,
        vertical_spacing=0.15,
        subplot_titles=('Price & MA', 'KD Indicator', 'MACD'),
        row_heights=[0.5, 0.25, 0.25],
        shared_xaxes=True,
    )
    
    # first row: price
    fig.add_trace(
        go.Candlestick(
            x=display_data['date'],
            open=display_data['open'],
            high=display_data['high'],
            low=display_data['low'],
            close=display_data['close'],
            name="Price",
            increasing_line_color='red', 
            decreasing_line_color='green'    
        ),
        row=1, col=1
    )

    ma_settings = [
        ('MA5', 'orange'),
        ('MA10', 'purple'),
        ('MA20', 'green')
    ]

    for ma_col, color in ma_settings:
        fig.add_trace(
            go.Scatter(
                x=display_data['date'],
                y=display_data[ma_col],
                name=ma_col,
                line=dict(color=color, width=2)
            ),
            row=1, col=1
        )

    signals = [
        ('entry_pre_pullback', 'EN_PP', 'orange'),
        ('entry_pullback', 'EN_P', 'green'),
        ('entry_breakout', 'EN_B', 'green'),
        ('entry_continuation', 'EN_C', 'green'),
        ('exit_trend', 'EX_T', 'red'),
        ('exit_emergency', 'EX_E', 'red'),
    ]

    signal_counts = {}
    y_gap = display_data['high'].max() * 0.015
    base_offset = display_data['high'].max() * 0.01

    for col, label, color in signals:
        mask = display_data[col] == True
        if mask.any():
            sig_dates = display_data.loc[mask, 'date']
            sig_highs = display_data.loc[mask, 'high']

            y_positions = []
            for d, h in zip(sig_dates, sig_highs):
                count = signal_counts.get(d, 0)
                y_pos = h + base_offset + (count * y_gap)
                y_positions.append(y_pos)
                signal_counts[d] = count + 1

            fig.add_trace(
                go.Scatter(
                    x=sig_dates,
                    y=y_positions,
                    mode="markers+text",
                    name=label,
                    text=label,
                    textposition="middle right",
                    marker=dict(
                        symbol='triangle-down',
                        size=10,
                        color=color
                    ),
                    textfont=dict(
                        color="black",
                        size=10,
                        family="Arial"
                    ),
                    fillcolor=color, 
                    hoverinfo='skip'
                ),
                row=1, col=1
            )
        
    # second row: kd
    fig.add_trace(
        go.Scatter(x=display_data['date'], y=display_data['K'], 
                  mode='lines', name='K line', line=dict(color='blue')),
        row=2, col=1
    )
    fig.add_trace(
        go.Scatter(x=display_data['date'], y=display_data['D'], 
                  mode='lines', name='D line', line=dict(color='red')),
        row=2, col=1
    )

    high_k = display_data[display_data['K'] > 80]
    fig.add_trace(
        go.Scatter(
            x=high_k['date'], 
            y=high_k['K'], 
            mode='markers', 
            name='K > 80',
            marker=dict(color='red', size=6),
            showlegend=False
        ),
        row=2, col=1
    )

    low_k = display_data[display_data['K'] < 20]
    fig.add_trace(
        go.Scatter(
            x=low_k['date'], 
            y=low_k['K'], 
            mode='markers', 
            name='K < 20',
            marker=dict(color='green', size=6),
            showlegend=False
        ),
        row=2, col=1
    )
    
    # third row: macd
    fig.add_trace(
        go.Scatter(x=display_data['date'], y=display_data['DIF'], 
                  mode='lines', name='DIF', line=dict(color='red')),
        row=3, col=1
    )
    fig.add_trace(
        go.Scatter(x=display_data['date'], y=display_data['MACD'], 
                  mode='lines', name='MACD', line=dict(color='blue')),
        row=3, col=1
    )
    colors = ['red' if x >= 0 else 'green' for x in display_data['MACD_hist']]
    fig.add_trace(
        go.Bar(x=display_data['date'], y=display_data['MACD_hist'], 
               name='MACD histogram', marker_color=colors, showlegend=False),
        row=3, col=1
    )
 
    # common settings
    fig.update_layout(
        template="plotly_white",
        plot_bgcolor='#F2F2F2',
        font=dict(family="Arial"),
        hovermode='x unified',
        xaxis_rangeslider_visible=False,
        height=850,
        showlegend=False,
        dragmode='pan',
        margin=dict(l=65, r=30, t=50, b=50),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_rangebreaks=[
            dict(values=pd.date_range(start=display_data["date"].min(), end=display_data["date"].max())
                 .difference(display_data["date"]))
        ]
    )

    fig.update_xaxes(
        hoverformat="%Y/%m/%d",
        gridcolor='white',
        showticklabels=True,
        tickformat="%m/%d",
        ticklabelstandoff=10,
        showspikes=True,
        spikemode='across',
        spikesnap='cursor',
        spikethickness=0.5,
        spikedash='dash',
        spikecolor="#000000",
        rangeslider_visible=False, 
    )

    fig.update_yaxes(
        ticks="outside",           
        ticklen=5,                 
        tickcolor='rgba(0,0,0,0)', 
        title_standoff=15,         
        automargin=True,
        gridcolor='white',
        showspikes=True,
        spikemode='toaxis+across',
        spikesnap='cursor',
        spikethickness=0.5,
        spikedash='dash',
        spikecolor="#000000",        
    )

    y_configs = {
        1: dict(autorange=True, fixedrange=False),
        2: dict(range=[0, 100], fixedrange=True),
        3: dict(autorange=True, fixedrange=False)
    }

    for row, config in y_configs.items():
        fig.update_yaxes(row=row, col=1, **config)

    legend_texts = [
        ' <span style="color:orange">― MA5</span> <span style="color:purple">― MA10</span> <span style="color:green">― MA20</span>',
        ' <span style="color:blue">― K line</span> <span style="color:red">― D line</span>   <span style="color:#555555">K值 > 80為紅點   K值 < 20為綠點</span>',
        ' <span style="color:red">― DIF</span> <span style="color:blue">― MACD</span>'
    ]

    for i, anno in enumerate(fig.layout.annotations):
        anno.text = f"<b>{anno.text}</b>  {legend_texts[i]}"
        anno.x = 0.005
        anno.xanchor = "left"
        anno.font.size = 14
        anno.y += 0.005
    
    return fig

if __name__ == '__main__':
    app.run(debug=True, port=8050)
