import os
import getpass
from google import genai
from google.genai import types
import json
import re
from datetime import datetime




if "GOOGLE_API_KEY" not in os.environ:
    os.environ["GOOGLE_API_KEY"] = getpass.getpass("Enter your Google AI API key: ")

client = genai.Client(api_key=os.environ["GOOGLE_API_KEY"])


# Create a dummy Python file
file_path = "C:\\lopu-kg-test\\project\\src\\main\\sql_for_pipelines\\xml_to_df.py"


# Read the content of the Python file
with open(file_path, "r") as f:
    python_code_text = f.read()

#print(f"Content from the python file: {python_code_text}")

prompt = "main\llm\py_script_extractor\py_prompt"
with open(prompt, "r") as f:
    prompt = f.read()


prompt = prompt.replace("[YOUR SQL QUERY HERE]", python_code_text)



response = client.models.generate_content(
    model="gemini-2.0-flash",
    config=types.GenerateContentConfig(
    # max_output_tokens=500,
        temperature=0.1),
    contents=prompt
)

# Write the model response to a JSON file
txt_path = "model_response.txt"
with open(txt_path, "w") as txt_file:
    txt_file.write(response.text)

print(f"Model response written to {txt_path}")
