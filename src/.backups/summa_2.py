import os
import getpass
from google import genai
import pandas as pd
from datetime import datetime
import time

def dsummarize_estonian_abstracts(csv_file_path):
    """
    Reads a CSV file, summarizes abstracts in Estonian using Gemini, and saves the summaries back to the CSV.

    Args:
        csv_file_path (str): The path to the CSV file.
    """
    if "GOOGLE_API_KEY" not in os.environ:
        os.environ["GOOGLE_API_KEY"] = getpass.getpass("Enter your Google AI API key: ")

    client = genai.Client(api_key=os.environ["GOOGLE_API_KEY"])

    try:
        df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        print(f"Error: File not found at {csv_file_path}")
        return
    except Exception as e:
        print(f"An error occurred while reading the CSV: {e}")
        return

    summaries = []
    for abstract in df["Abstract Note"]:
        time.sleep(10)
        if pd.isna(abstract): #Handle NaN values.
            summaries.append("Abstrakti pole saadaval.")
            continue

        prompt = f"""
        Palun tee järgnevast tekstist 2-3 lauseline kokkuvõte eesti keeles, tegemist on uurimistöö abstraktidega.
        {abstract}
        """

        try:
            response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
            summaries.append(response.text)
        except Exception as e:
            print(f"An error occurred during summarization: {e}")
            summaries.append("Kokkuvõtte tegemisel tekkis viga.")
            # Optionally, you might want to stop processing if an error occurs.
            # return

    df["Summary"] = summaries
    try:
        df.to_csv(csv_file_path, index=False)
        print(f"Summaries saved to {csv_file_path}")
    except Exception as e:
        print(f"An error occurred while saving the CSV: {e}")

# Example usage (replace 'your_file.csv' with your actual file path):
# summarize_estonian_abstracts('your_file.csv')

dsummarize_estonian_abstracts("C:\lopu-kg-test\project\src\.backups\\two.csv")