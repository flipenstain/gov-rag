# 📁 Project Structure

This repository contains various scripts and data files related to data generation, database creation, and working with LLM responses.

## 📂 Directory Overview

```
src/
│── data/                               # 📂 TPC-DI data
│── divider/dlls/                       # 📂 DuckDB model sql and comment files
│       ├── duck_db_main_model.py       # 🛢️ Database Creation
│       ├── duck_db_step_by_step.ipynb  # 🛢️ Transformation logic
│── main/                               # 🧠 Pipelines, dependencies
│── templates/                          # 📊 Prompt templates
memgraph/                               # 📂 Memgraph logic
agentic/                                # 📂 Agentic workflow
```


# 🚀 Generating Source Files with DIGen.jar

This document outlines the steps taken to utilize `DIGen.jar` for generating source files, following the guidance provided in [this repository](https://github.com/stewartbryson/dbt-tpcdi?tab=readme-ov-file#using-digenjar-to-generate-sourcefiles).

## 📥 Downloading the DIGen.jar Tool

The `DIGen.jar` Java program is obtained by:

1.  Filling out a form on the [TPC-DI website](https://www.tpc.org/TPC_Documents_Current_Versions/download_programs/tools-download-request5.asp?bm_type=TPC-DI&bm_vers=1.1.0&mode=CURRENT-ONLY).
2.  Clicking the download link provided in the subsequent email.

Once the downloaded ZIP file is extracted, a minor adjustment is necessary for macOS users due to its case-insensitive file system:

```bash
unzip 66a2b600-af36-4198-bfbc-c94c40cc22af-tpc-di-tool.zip && \
mv Tools/PDGF Tools/pdgf && \ 
cd Tools
```

## ⚠️ Java Version Compatibility
It was observed that executing the DIGen.jar file required Java version 1.8. Attempts with newer Java versions were unsuccessful.

To address this:

Installed Azul Zulu Java 1.8: Downloaded from Azul Zulu Java 1.8 Downloads.
Utilized jEnv for Windows: Although the guide mentions macOS, on a Windows environment, jEnv for Windows was employed to manage and set the local Java version.
With Java 1.8 configured, the help context of the DIGen.jar can be viewed using the following commands:

```Bash

jenv add /Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home && \
jenv local 1.8 && \
java -jar DIGen.jar --help
This command outputs the following usage information:
```

## ⚙️ Running the DIGen.jar Utility
To execute the DIGen.jar and generate the data files, use the following command in your command prompt or terminal:

```Bash
java -cp "DIgen.jar;commons-cli-1.9.0.jar" -jar DIgen.jar
```
This utility will generate various data files in different formats. The amount of data generated is determined by a scaling factor, which acts as a multiplier. You can specify this factor using the -sf option as shown in the help context.

📂 Output data will be stored in:
```Bash
..Tools\PDGF\output

# move the files to 
src/
│── data/                           # 📂 TPC-DI data

```


## 🛢️ DuckDB & Lineage Extraction Workflow

This section details the execution flow for the DuckDB and lineage extraction process.

### ⚙️ DuckDB Model Creation

To create the main DuckDB model, navigate to the `src/main/duckdb/` directory and execute the following Python script:

```bash
cd src/main/duckdb/
python duck_db_main_model.py
```

### 밟 Step-by-Step Transformations and SQL Generation

Next, execute the Jupyter Notebook located in the same directory to perform transformations and save the logic into SQL files:

```bash
jupyter notebook duck_db_step_by_step.ipynb
```

**(Note:** This command will typically open the Jupyter Notebook in your web browser. Follow the instructions and execute the cells within the notebook.)

### 📄 Lineage Extraction Prompts

The prompts used for lineage extraction are located in the `src\templates\` directory. These files are utilized by the lineage extraction scripts.

### 🧠 Lineage Extraction Script (Initial Implementation)

To run the first implementation of the lineage extractor using a regular prompt, execute the following Python script:

```bash
python src/main/llm/lineage_extractor_v2.py
```

### 📊 Token Usage Calculation

To calculate the token usage for the LLM interactions during lineage extraction, run the following script:

```bash
python src/main/llm/calc_token_usage.py
```

### 🔗 Pipeline and Dependency Configurations

The following JSON files define the pipelines and their dependencies:

* `src\main\pipeline_dependency.json`: Specifies dependencies between pipelines.
* `src\main\pipeline_mapping.json`: Maps pipelines to their components or scripts.
* `src\main\script_dependency.json`: Defines dependencies between individual scripts.

**(Note:** These files are typically used by other parts of the system or pipeline orchestration and are not directly executed via a `python` command here. They serve as configuration for how the lineage extraction and related processes are organized.)

**Summary of Execution Order:**

1.  **DuckDB Model:** `cd src/main/duckdb/` followed by `python duck_db_main_model.py`
2.  **Transformations & SQL:** `jupyter notebook duck_db_step_by_step.ipynb` (execute cells within the notebook)
3.  **Lineage Extraction:** `python src/main/llm/lineage_extractor_v2.py`
4.  **Token Calculation:** `python src/main/llm/calc_token_usage.py`
5.  **Configuration Files:** (`src\main\pipeline_dependency.json`, `src\main\pipeline_mapping.json`, `src\main\script_dependency.json` are used for configuration, not direct execution here).


# 🧠 Set up and fill Memgraph 

### Follow guide from [Memgraph](https://memgraph.com/docs/getting-started/install-memgraph/docker).



## 🚀 Running Lineage Extraction and Agentic Workflows

This section outlines the steps to execute the lineage extraction and agentic workflows.

### 🛠️ Prerequisites: Development Container

Ensure you are running these scripts within a **new development container** for a consistent environment.

### ➡️ First Lineage Extractor Flow

To execute the initial lineage extraction process, navigate to the `memgraph/` directory within your development container and run the following script:

```bash
cd memgraph/
python memgraph_process.py
```

### ▶️ Agentic Workflow

For the agentic workflow, follow these steps:

1.  **Navigate to the `agentic/` directory:**

    ```bash
    cd agentic/
    ```

2.  **Execute the agentic workflow script:**

    ```bash
    python agent_v3.py
    ```

3.  **Run the Memgraph processing script for agentic lineage:**

    Open a **new terminal** or background the previous process and navigate to the `memgraph/` directory:

    ```bash
    cd ../memgraph/
    ```

4.  **Execute the script to populate Memgraph with agentic lineage data:**

    ```bash
    python memgraph_process_v5_agentic.py
    ```

**Summary of Execution Order:**

1.  **First Lineage:** `cd memgraph/` followed by `python memgraph_process.py`
2.  **Agentic Workflow:**
    * `cd agentic/` followed by `python agent_v3.py`
    * (In a new terminal/background) `cd memgraph/` followed by `python memgraph_process_v5_agentic.py`

This structured approach ensures that the necessary scripts for each workflow are executed in the correct context and order.