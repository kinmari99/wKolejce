import pandas as pd
from db import get_engine

df = pd.read_csv("data/cities/miasta.csv")

engine = get_engine()

df.to_sql(
    "city_coordinates",
    engine,
    schema="ref",
    if_exists="replace",
    index=False
)