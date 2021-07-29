from flask import Flask, render_template, request
import pandas as pd
import numpy as np
import plotly.graph_objs as go
import json

from src.connect_cassandra import *
from streamz.dataframe import PeriodicDataFrame
from bokeh.embed import components
from bokeh.models import ColumnDataSource, HoverTool, PrintfTickFormatter
from bokeh.plotting import figure
from bokeh.transform import factor_cmap

app = Flask(__name__)

# Connect to database upon loading
session = connect_to_database("social_media")


def random_datapoint(**kwargs):
    return pd.DataFrame(
        {"a": np.random.random(1), "b": np.random.random(1)},
        index=[pd.Timestamp.now()],
    )


@app.route("/", methods=["GET"])
def homepage():
    random_chart = create_plot()
    return render_template("main.html", random_chart=random_chart)


def create_plot():

    df = PeriodicDataFrame(random_datapoint, interval="5s")

    return df.hvplot(backlog=100)


if __name__ == "__main__":
    app.run(debug=True)
