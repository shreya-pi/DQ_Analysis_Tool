### File-by-File Explanation

 **`input_and_output_files/`**: Centralizes file-based data.
    *   **`original_schema.txt`**: **Crucial Input File.** This must contain the `CREATE TABLE` DDL for the tables you want to describe with the Cortex feature. 
    *   **`formatted_schema.md` / `schema.json`**: These provide alternative, more structured ways to store or view the schema information.`col_desc.py` reads from this file.
    *   **`table_desc.txt`**: The default output file where the AI-generated column descriptions are saved.
    *   **`views.md` / `views.json`**: Documentation explaining the SQL logic for the required Snowflake views (`_duplicate_record`, `_clean_view`), essential for other developers to understand the project's dependencies.
---
*   **`logs/`**: A dedicated directory for storing runtime log files, keeping the root directory clean.