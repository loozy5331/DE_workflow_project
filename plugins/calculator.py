# moving average
def get_moving_average(prices, window=1):
    return prices.rolling(window, min_periods=1).sum()/window

# maximum drawdown(mdd)
def get_maximum_drawdown(prices, window=20):
    max_in_window = prices.rolling(window, min_periods=1).max()
    drawdown = ((prices/max_in_window) - 1.0) * 100
    mdd = drawdown.rolling(window, min_periods=1).min()

    return mdd