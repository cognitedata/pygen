from pathlib import Path

import pandas as pd


def main():
    # Get all CSV files in the current directory
    csv_files = [f for f in Path(".").iterdir() if f.suffix == ".csv"]

    combined_data = []

    # Loop through each CSV file and append its data to the combined DataFrame
    for file_path in csv_files:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Add a new column with the movie title
        df["Movie Title"] = file_path.stem

        # Append the data to the combined DataFrame
        combined_data.append(df)

    # Save the combined data to a new CSV file
    pd.concat(combined_data, axis=0).to_csv("ratings.csv", index=False)


if __name__ == "__main__":
    main()
