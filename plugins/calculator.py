from datetime import timedelta

def get_date_range(curr_dt, t_delta=timedelta(days=1)):
    """
        curr_dt에서 t_delta 전에 있는 시간과 반환
    """
    before_dt = curr_dt - t_delta

    str_before_dt = before_dt.strftime("%Y-%m-%d")
    str_curr_dt = curr_dt.strftime("%Y-%m-%d")

    return str_before_dt, str_curr_dt

def get_moving_average(df, window=200):
    """
        window 간격의 이동평균
    """
    df["mv200"] = df["adj_close"].rolling(window, min_periods=1).sum()/window

    return df

def get_low_n_high_52week(df):
    """
        52주(약 1년) 간격으로 가격을 비교

        - low_52w : 가장 낮았을 때의 가격 비
        - high_52w: 가장 낮았을 때의 가격 비
    """
    dates = df.index
    first_date = dates[0] + timedelta(weeks=52)

    for curr_dt, row in df.iterrows():
        if curr_dt < first_date:
            continue

        str_before_dt, str_curr_dt = get_date_range(curr_dt, t_delta=timedelta(weeks=52))
        date_range = df[(str_before_dt <= df.index) & (df.index <= str_curr_dt)]

        # 52w_high
        max_price = date_range["adj_close"].max()
        df.loc[str_curr_dt, "high_52w"] = ((max_price-row.adj_close)/max_price) * 100

        # 52w_low
        min_price = date_range["adj_close"].min()
        df.loc[str_curr_dt, "low_52w"] = ((row.adj_close-min_price)/min_price) * 100

    return df