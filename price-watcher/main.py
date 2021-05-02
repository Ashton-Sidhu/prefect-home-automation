import requests

from bs4 import BeautifulSoup

from prefect import task, Flow, Parameter
from prefect.tasks.notifications.email_task import EmailTask
from prefect.tasks.control_flow import case

@task
def get_price(url):
    """
    Get price of product from a URL.

    TODO: Change this to get the price of your product!
    """

    html = requests.get(url).text

    soup = BeautifulSoup(html, features="html.parser")

    price_tag = soup.find("span", attrs={"class": "value"})

    return float(price_tag.attrs["content"])


@task
def create_message(price: float) -> str:
    """Creates the message for the email. HTML is supported as per Prefect docs
    https://docs.prefect.io/api/latest/tasks/notifications.html#emailtask"""

    message = """
    Hi,
    \n<br>
    Your item has a new price of : {}
    \n<br>
    """

    return message.format(price)

@task
def is_different(price_point: int, price: float) -> bool:
    """Checks to see if the price has changed"""
    return float(price_point) != price

email_task = EmailTask(subject="Price Check")

# Schedule is set via UI
# Can't schedule jobs with required parameters programatically.
with Flow("Check Price", schedule=None) as flow:

    # These are set via the Prefect UI under the flow settings
    email = Parameter("email", required=True)
    price_point = Parameter("price_point", required=True)
    url = Parameter("url", required=True)

    # Get the price from the site
    price = get_price(url)

    # Is the price different
    cond = is_different(price_point, price)

    # If price is different, send email
    with case(cond, True):
        msg = create_message(price)

        # Send email
        email_task(msg=msg, email_to=email)

flow.register(project_name="Price-Checker")
