import json
# --- Reading in sql pipeline to process mapping ---
json_file_pipe_dep = "/app/src/main/pipeline_dependency.json"
with open(json_file_pipe_dep, "r", encoding="utf-8") as f:
                PIPELINE_DEPENDENCY = json.load(f)

json_file_script_dep = "/app/src/main/script_dependency.json"
with open(json_file_pipe_dep, "r", encoding="utf-8") as f:
                SCRIPT_DEPENDENCY = json.load(f)

pipe_deps_dict = {
pipeline: dependant
for pipeline, dependant in SCRIPT_DEPENDENCY.items()
for dependant in dependant
}

script_depts_dict = {
script: dependant
for script, dependant in PIPELINE_DEPENDENCY.items()
for dependant in dependant
}

for pipe, dependant in pipe_deps_dict.items():
    print(f"""
                MATCH (p:Pipeline {name: $pipe})
                MATCH (p2:Pipeline {name: $dependant})
                MERGE (p)-[:DEPENDS_ON]->(p2)
                """)