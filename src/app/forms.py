from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, DateField


class TickerForm(FlaskForm):
    ticker = StringField("Ticker")
    start_date = DateField("StartDate")
    end_date = DateField("EndDate")
    submit = SubmitField("Generate")
