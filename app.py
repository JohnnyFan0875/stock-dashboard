import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html, ctx
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import numpy as np
from dateutil.relativedelta import relativedelta

# load data
df = pd.read_parquet('data/processed/daily.parquet')
df = df.sort_values(["stock_id", "date"])

# define functions
def calculate_kd(df, n=9):
    low_n  = df['low'].rolling(n, min_periods=1).min()
    high_n = df['high'].rolling(n, min_periods=1).max()

    denom = high_n - low_n
    denom = denom.replace(0, np.nan)

    rsv = 100 * (df['close'] - low_n) / denom

    df['K'] = rsv.ewm(alpha=1/3, adjust=False).mean()
    df['D'] = df['K'].ewm(alpha=1/3, adjust=False).mean()

    return df


def calculate_macd(df, fast=12, slow=26, signal=9):
    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()

    df['DIF'] = ema_fast - ema_slow
    df['MACD'] = df['DIF'].ewm(span=signal, adjust=False).mean()
    df['MACD_hist'] = df['DIF'] - df['MACD']

    return df

def calculate_signals(df):
    # MA
    df['ma5'] = df['close'].rolling(5).mean()
    df['ma10'] = df['close'].rolling(10).mean()
    df['ma20'] = df['close'].rolling(20).mean()
    
    # 趨勢定義 (Trend OK)
    df['trend_ok'] = (df['DIF'] > 0) & (df['MACD'] > 0) & (df['DIF'] > df['MACD'])
    
    # KD 交叉與距離上次交叉的天數 (模擬 ta.barssince)
    df['kd_cross'] = (df['K'] > df['D']) & (df['K'].shift(1) <= df['D'].shift(1))
    # 計算自上次交叉以來的天數
    df['bars_after_kd_cross'] = df['kd_cross'].cumsum()
    df['bars_after_kd_cross'] = df.groupby('bars_after_kd_cross').cumcount()
    # 如果還沒發生過交叉，設為大數
    df.loc[df['kd_cross'].cumsum() == 0, 'bars_after_kd_cross'] = 999

    # Pullback: 趨勢好 + KD剛交叉2天內 + KD張口擴大 + 股價高於MA10
    kd_gap = df['K'] - df['D']
    df['entry_pullback'] = df['trend_ok'] & \
                           (df['bars_after_kd_cross'] <= 2) & \
                           (kd_gap > kd_gap.shift(1)) & \
                           (df['K'] < 80) & \
                           (df['close'] > df['ma10'])

    # Breakout: 趨勢好 + K>50 + 帶量/長紅突破MA10
    df['entry_breakout'] = df['trend_ok'] & (df['K'] > 50) & \
                           (df['close'] > df['ma10']) & (df['close'].shift(1) <= df['ma10'])

    # Continuation: 持續走強
    df['entry_continuation'] = df['trend_ok'] & (df['K'] > 50) & (df['K'] < 80) & (df['close'] > df['ma10'])

    # --- Exit 邏輯 ---
    # 模擬 ta.barssince(entry) > 5
    df['any_entry'] = df['entry_pullback'] | df['entry_breakout'] | df['entry_continuation']
    df['bars_since_entry'] = df['any_entry'].cumsum()
    df['bars_since_entry'] = df.groupby('any_entry').cumcount() 
    
    exit_allowed = df['bars_since_entry'] > 5
    exit_price = (df['close'] < df['ma20']) & (df['close'].shift(1) < df['ma20'])
    exit_macd = (df['DIF'] < df['MACD']) & (df['MACD_hist'] < 0) & (df['MACD_hist'] < df['MACD_hist'].shift(1))
    
    df['exit_trend'] = (exit_price | exit_macd) & exit_allowed
    df['exit_emergency'] = df['close'] < (df['ma20'] * 0.97)

    return df


# Dash app
app = dash.Dash(__name__)

# define stock options
stock_options = []
unique_stocks = df[['stock_id', 'stock_name']].drop_duplicates()
for _, row in unique_stocks.iterrows():
    stock_options.append({'label': f"{row['stock_id']} - {row['stock_name']}", 'value': row['stock_id']})

app.layout = html.Div([
    html.H1("Dashboard", style={'textAlign': 'center'}),
    
    dcc.Dropdown(
        id='stock-dropdown',
        options=stock_options,
        value='2330',  # default value
        style={'width': '50%', 'margin': 'auto', 'marginBottom': '50px', 'marginTop': '50px'}
    ),

    html.Div([
        html.Button("最近一個月", id="btn-1m", n_clicks=0, style={'margin': '5px'}, className="time-btn"),
        html.Button("最近三個月", id="btn-3m", n_clicks=0, style={'margin': '5px'}, className="time-btn"),
        html.Button("最近六個月", id="btn-6m", n_clicks=0, style={'margin': '5px'}, className="time-btn"),
        html.Button("最近一年", id="btn-1y", n_clicks=0, style={'margin': '5px'}, className="time-btn"),
        html.Button("全部資料", id="btn-all", n_clicks=0, style={'margin': '5px'}, className="time-btn"),
    ], style={'textAlign': 'right', 'marginRight': '100px'}),

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
    ], style={'width': '90%', 'margin': 'auto', 'padding': '20px'}),
])

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

from dateutil.relativedelta import relativedelta # 建議匯入此套件處理日期加減

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

# 1. 讓圖表拖移後，反向更新 Slider 的 Value
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
    Output('stock-charts', 'figure'),
    [Input('stock-dropdown', 'value'),
     Input('date-slider-top', 'value')]
)
def update_charts(selected_stock, date_range):

    if not date_range or not isinstance(date_range, list) or len(date_range) < 2:
        raise PreventUpdate
    
    stock_data = df[df['stock_id'] == selected_stock].sort_values('date').copy()
    if stock_data.empty:
        return go.Figure()
    
    start_dt = pd.to_datetime(date_range[0], unit='s')
    end_dt = pd.to_datetime(date_range[1], unit='s')
    stock_data = stock_data[(stock_data['date'] >= start_dt) & (stock_data['date'] <= end_dt)]
    
    stock_data = calculate_kd(stock_data)
    stock_data = calculate_macd(stock_data)
    
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
            x=stock_data['date'],
            open=stock_data['open'],
            high=stock_data['high'],
            low=stock_data['low'],
            close=stock_data['close'],
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

    for ma, color in ma_settings:
        stock_data[ma] = stock_data['close'].rolling(int(ma[2:])).mean()
        fig.add_trace(
            go.Scatter(
                x=stock_data['date'],
                y=stock_data[ma],
                name=ma,
                line=dict(color=color, width=2)
            ),
            row=1, col=1
        )

    stock_data = calculate_signals(stock_data)
    signals = [
        ('entry_pullback', 'EN_P', 'green'),
        ('entry_breakout', 'EN_B', 'green'),
        ('entry_continuation', 'EN_C', 'green'),
        ('exit_trend', 'EX_T', 'red'),
        ('exit_emergency', 'EX_E', 'red'),
    ]

    signal_counts = {}
    y_gap = stock_data['high'].max() * 0.025
    base_offset = stock_data['high'].max() * 0.01

    for col, label, color in signals:
        mask = stock_data[col] == True
        if mask.any():
            sig_dates = stock_data.loc[mask, 'date']
            sig_highs = stock_data.loc[mask, 'high']

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
                    textposition="top center",
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
        go.Scatter(x=stock_data['date'], y=stock_data['K'], 
                  mode='lines', name='K line', line=dict(color='blue')),
        row=2, col=1
    )
    fig.add_trace(
        go.Scatter(x=stock_data['date'], y=stock_data['D'], 
                  mode='lines', name='D line', line=dict(color='red')),
        row=2, col=1
    )

    high_k = stock_data[stock_data['K'] > 80]
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

    low_k = stock_data[stock_data['K'] < 20]
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
        go.Scatter(x=stock_data['date'], y=stock_data['DIF'], 
                  mode='lines', name='DIF', line=dict(color='red')),
        row=3, col=1
    )
    fig.add_trace(
        go.Scatter(x=stock_data['date'], y=stock_data['MACD'], 
                  mode='lines', name='MACD', line=dict(color='blue')),
        row=3, col=1
    )
    colors = ['red' if x >= 0 else 'green' for x in stock_data['MACD_hist']]
    fig.add_trace(
        go.Bar(x=stock_data['date'], y=stock_data['MACD_hist'], 
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
            dict(values=pd.date_range(start=stock_data["date"].min(), end=stock_data["date"].max())
                 .difference(stock_data["date"]))
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
