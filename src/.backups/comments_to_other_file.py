import os
import re

def extract_comments_from_folder(folder_path):
    """
    Extracts comments from all .sql files in a folder and creates corresponding .comment files.

    Args:
        folder_path (str): The path to the folder containing .sql files.
    """
    print(folder_path)
    if not os.path.isdir(folder_path):
        print(f"Error: {folder_path} is not a directory.")
        return

    for filename in os.listdir(folder_path):
        if filename.endswith(".sql"):
            sql_file_path = os.path.join(folder_path, filename)

            if not sql_file_path.endswith(".sql"):
                print(f"Error: {sql_file_path} is not a .sql file.")

            comment_file_path = sql_file_path.replace(".sql", ".comment")
        
            extract_and_remove_comment_on_lines(sql_file_path, comment_file_path)



def extract_and_remove_comment_on_lines(sql_file_path, comment_file_path):
    """
    Extracts 'COMMENT ON' lines to a separate file and removes them from the original.

    Args:
        sql_file_path (str): Path to the SQL file.
        comment_file_path (str): Path to the comment file.
    """
    try:
        with open(sql_file_path, "r") as sql_file:
            lines = sql_file.readlines()

        comment_lines = []
        cleaned_lines = []

        for line in lines:
            if re.search(r"^\s*COMMENT ON.*$", line):
                comment_lines.append(line)
            else:
                cleaned_lines.append(line)

        with open(comment_file_path, "w") as comment_file:
            comment_file.writelines(comment_lines)

        with open(sql_file_path, "w") as sql_file:
            sql_file.writelines(cleaned_lines)

        print(f"Comment ON lines extracted and removed from {sql_file_path}")

    except FileNotFoundError:
        print(f"Error: File not found at {sql_file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")


test_sql_file_path = r"src\divider\ddls"
extract_comments_from_folder(test_sql_file_path)
