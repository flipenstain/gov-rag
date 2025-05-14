
import re
import json
import pandas as pd

def summarize_lineage_issues(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Split by script blocks: script_name followed by JSON object
    blocks = re.findall(r'([^\n]+\.sql):\s*\{(.*?)\}(?=\s*[^\n]+\.sql:|\Z)', content, re.DOTALL)

    summary = []
    total_columns = 0
    total_ok_columns = 0

    for script_name, json_block in blocks:
        # Wrap back to full JSON format
        json_str = "{" + json_block.strip() + "}"
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            summary.append({
                "script": script_name,
                "errors": [f"JSON parse error: {e}"],
                "error_count": 1,
                "total_columns": 0,
                "ok_columns": 0
            })
            continue

        column_count = len(data)
        error_columns = [col for col, issues in data.items() if issues]
        ok_columns = column_count - len(error_columns)

        total_columns += column_count
        total_ok_columns += ok_columns

        summary.append({
            "script": script_name,
            "errors": error_columns,
            "error_count": len(error_columns),
            "total_columns": column_count,
            "ok_columns": ok_columns
        })

    # Print table
    print(f"{'Script':<35} | {'Errors'}")
    print("-" * 80)
    for entry in summary:
        if entry["error_count"] == 0:
            status = "✅ OK"
        elif "JSON parse error" in entry["errors"][0]:
            status = f"❌ {entry['errors'][0]}"
        else:
            status = f"❌ {entry['error_count']} issue(s): {', '.join(entry['errors'])}"
        print(f"{entry['script']:<35} | {status}")

    # Print totals
    print("\n--- Summary ---")
    print(f"Total scripts reviewed:   {len(summary)}")
    print(f"Total columns reviewed:   {total_columns}")
    print(f"Total columns without issues: {total_ok_columns}")
    print(f"Total columns with issues:    {total_columns - total_ok_columns}")

    
    return summary

def extract_detailed_issues(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    blocks = re.findall(r'([^\n]+\.sql):\s*\{(.*?)\}(?=\s*[^\n]+\.sql:|\Z)', content, re.DOTALL)

    detailed_issues = []

    for script_name, json_block in blocks:
        json_str = "{" + json_block.strip() + "}"
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            detailed_issues.append({
                "script": script_name,
                "column": None,
                "issue_description": f"JSON parse error: {e}"
            })
            continue

        # Extracting column-level issues
        for column, issues in data.items():
            if issues:  # assuming 'issues' is a list of descriptions
                for issue_desc in issues:
                    detailed_issues.append({
                        "script": script_name,
                        "column": column,
                        "issue_description": issue_desc
                    })

    return pd.DataFrame(detailed_issues)


file_path = "C:\lopu-kg-test\project\EVALUATOR_OUTPUT.txt"

sum = summarize_lineage_issues(file_path)

df = pd.DataFrame(sum)

print(df)


detail_issues = extract_detailed_issues(file_path)

print(detail_issues)