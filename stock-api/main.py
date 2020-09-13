import time
import numpy as np
import pandas as pd

from collections import ChainMap
from ctypes import *
from datetime import date
from typing import List, Dict

from .conf import EMAIL_TO

from prefect import flatten, task, Flow
from prefect.tasks.notifications.email_task import EmailTask
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock


def color_gains_loss(val):
    """
    Colours positive gains as green and negative as green
    """
    color = 'red' if val < 0 else 'green'

    return 'color: %s' % color

@task
def load_stocks() -> pd.DataFrame:

    return pd.read_csv("./stocks.csv")


@task
def get_price(tickers: List[str]) -> Dict[str, str]:
    """Gets the adjusted closing price of all stocks."""

    lib = cdll.LoadLibrary("./stock-api.so")

    lib.getPrice.argtypes = [c_char_p, c_char_p]
    lib.getPrice.restype = c_char_p

    curr_date = str(date.today())

    prices = []
    for ticker in tickers:
        prices.append(
            {ticker: lib.getPrice(ticker.encode(), curr_date.encode()).decode()}
        )

    time.sleep(70)

    return prices


@task
def split_stocks(stock_list: list, steps: int) -> List[List[Dict[str, str]]]:
    """Splits the stocks into groups of `steps` to account for AlphaVantage API"""

    return [stock_list[i : i + steps] for i in range(0, len(stock_list), steps)]


@task
def concat_stocks(stock_lists: List[Dict]) -> Dict[str, str]:
    """Concats a list of small dictionaries of `stock`:`price` into one dictionary"""

    return dict(ChainMap(*stock_lists))


@task
def add_price(
    stocks: pd.DataFrame, stocks_price_mapping: Dict[str, str]
) -> pd.DataFrame:
    """Adds the closing price to the data"""

    stocks["closing_price"] = stocks["stock"].map(stocks_price_mapping)

    return stocks


@task
def get_difference(stocks: pd.DataFrame) -> pd.DataFrame:
    """Calculates the gains or loss for each stock."""

    stocks = stocks.astype({"closing_price": "float", "initial_value": "float"})

    stocks["gain_loss"] = stocks["closing_price"] - stocks["initial_value"]
    stocks["gain_loss"] = stocks["gain_loss"].round(2)

    return stocks


@task
def get_direction(stocks: pd.DataFrame) -> pd.DataFrame:
    """Gets whether the stock was `Up` or `Down` from the initial value."""

    stocks["direction"] = np.where(stocks["gain_loss"] >= 0, "Up", "Down")

    return stocks


@task
def create_message(stocks: pd.DataFrame) -> str:
    """Creates the message for the email. HTML is supported as per Prefect docs
    https://docs.prefect.io/api/latest/tasks/notifications.html#emailtask"""

    message = """
    Hi,
    \n<br>
    Here is your stock holding update for the week:
    \n<br>
    {}
    """

    stocks = stocks.rename(
        {
            "stock": "Stock",
            "initial_value": "Bought At",
            "closing_price": "Current Price",
            "gain_loss": "Difference"
        },
        axis="columns"
    )

    stocks_html = (stocks.style
        .applymap(color_gains_loss, subset=["Difference"])
        .format({"Bought At": "${:20,.2f}", "Current Price": "${:20,.2f}", "Difference": "${:20,.2f}"})
        .hide_index()
    )
    
    return message.format(stocks_html.render())


schedule = Schedule(clocks=[CronClock("0 21 * * 5")])

email_task = EmailTask(subject="Weekly Holdings Update", email_to=EMAIL_TO)

with Flow("Stock-API", schedule=schedule) as flow:

    # Load the stocks + the initial value
    # CSV SCHEMA: stock,initial_value
    stocks = load_stocks()

    # Split the stocks into even groups of 5 as the Vantage API
    # only allows 5 api calls per minute
    split_stocks = split_stocks(stocks["stock"], 5)

    # Get the adjusted closing price of each group of stocks
    stock_prices = get_price.map(split_stocks)

    # Concat the stocks back into a single list
    stocks_price_mapping = concat_stocks(flatten(stock_prices))

    # Add closing price to dataframe
    stocks = add_price(stocks, stocks_price_mapping)

    # Calculate gain or loss based off initial price
    stocks = get_difference(stocks)

    # Create the email message
    message = create_message(stocks)

    # Send email
    email_task(msg=message)

flow.register(project_name="Portolio Updater")
