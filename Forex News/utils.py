import os
import re
import pandas as pd

def reformat_scraped_data(data, month):
    """
    Reformat scraped data into a structured DataFrame and save it as a CSV file.
    Args:
        data (list): Scraped data as a list of rows.
        month (str): Month of the data being scraped.
    Returns:
        pd.DataFrame: Reformatted DataFrame.
    """
    current_date = ''
    current_time = ''
    structured_rows = []

    for row in data:
        if len(row) == 1 or len(row) == 5:  # New date
            current_date = row[0]
        elif len(row) == 4:  # Event row
            current_time = row[0]
        elif len(row) > 1:
            structured_rows.append([current_date, current_time, *row[-3:]])  # Append event data

    df = pd.DataFrame(structured_rows, columns=['date', 'time', 'currency', 'impact', 'event'])
    os.makedirs("news", exist_ok=True)
    csv_path = f"news/{month}_news.csv"
    df.to_csv(csv_path, index=False)
    print(f"Saved raw data to: {csv_path}")
    return df
