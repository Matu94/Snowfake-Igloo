# ❄️ Igloo - Low-Code Object Management in Snowflake

> **A streamlined, low-code object management solution built entirely inside Snowflake using Streamlit.**

![Status](https://img.shields.io/badge/Status-In%20Development-yellow) ![Snowflake](https://img.shields.io/badge/Built%20on-Snowflake-blue) ![Streamlit](https://img.shields.io/badge/Frontend-Streamlit-red) ![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python&logoColor=white)

## 📖 Project Summary

**Igloo** is a native Snowflake application designed to simplify database object management. It provides a **low-code GUI** that allows data engineers, analysts, and administrators to visually create, modify, and deploy Snowflake objects—specifically **Tables**, **Views**, and **Dynamic Tables**—without writing complex DDL manually. It runs directly within Snowflake as a Streamlit app, ensuring data security and seamless integration with your existing Snowflake account.

## 🚀 Key Features

### 1. Visual Object Builders
Create objects through intuitive wizards instead of raw SQL code.
*   **Tables**: Define columns, data types, and constraints (PK, Nullable) via a grid interface.
*   **Views**: Build standard views by selecting source schemas/tables and defining column mappings.
*   **Dynamic Tables**: Visually configure refresh schedules (lag) and target warehouses for automated pipelines.

### 2. Low-Code Data Editor
*   **Interactive Grid**: Modify column definitions using a spreadsheet-like interface.
*   **Smart Type Detection**: Automatically suggests data types based on source table metadata.

### 3. Integrated Deployment & Version Control
*   **One-Click Deploy**: Generate and execute production-ready DDL directly in Snowflake.
*   **Preview Mode**: Review the generated SQL before it runs.
*   **Git Integration**: Automatically push DDL changes to a connected GitHub repository upon deployment, ensuring your code is always version-controlled.

### 4. Connection Dashboard
*   **Live Status**: Monitor your current connection, role, warehouse, and database context.
*   **Secure Auth**: Uses Snowflake's native authentication or key-pair authentication for local development.

---

## 🛠️ Installation in Snowflake

Igloo is designed to run as a **Streamlit in Snowflake (SiS)** app. Follow these steps to deploy it:

### Prerequisites
*   A Snowflake account with permissions to create Stages and Streamlit apps.
*   A warehouse to run the app.

### Steps

1.  **Prepare the Environment**
    *   Log in to Snowsight.
    *   Create a database and schema for the app (e.g., `TOOLS.IGLOO`).

2.  **Create a Named Stage**
    ```sql
    CREATE STAGE TOOLS.IGLOO.APP_STAGE;
    ```

3.  **Upload Files**
    *   Upload all project files to the stage, maintaining the directory structure:
        *   `streamlit_app.py` (Root)
        *   `environment.yml` (Root)
        *   `components/` (Folder)
        *   `utils/` (Folder)
        *   `models/` (Folder)

4.  **Create the Streamlit App**
    Run the following command in a SQL worksheet:
    ```sql
    CREATE STREAMLIT TOOLS.IGLOO.IGLOO_APP
    ROOT_LOCATION = '@TOOLS.IGLOO.APP_STAGE'
    MAIN_FILE = '/streamlit_app.py'
    QUERY_WAREHOUSE = 'YOUR_WAREHOUSE';
    ```

5.  **Run the App**
    *   Navigate to **Streamlit** in the Snowsight menu.
    *   Click on **Igloo App** to launch it.

---

## 💻 Local Development

To run Igloo locally for development:

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/Matu94/Snowfake-Igloo.git
    cd Snowfake-Igloo
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure Credentials:**
    Create `.streamlit/secrets.toml`:
    ```toml
    [snowflake]
    user = "YOUR_USER"
    password = "YOUR_PASSWORD"
    account = "YOUR_ACCOUNT"
    role = "YOUR_ROLE"
    warehouse = "YOUR_WAREHOUSE"
    database = "YOUR_DATABASE"
    schema = "YOUR_SCHEMA"

    [github]
    token = "YOUR_GITHUB_TOKEN"
    repo_name = "your/repo"
    branch = "main"
    ```

4.  **Run the app:**
    ```bash
    streamlit run streamlit_app.py
    ```

---

## 📜 License

This project is open-source.

## Show Your Support
If you find Igloo helpful, consider giving it a star on GitHub! It helps others discover the project. Also you can [![Ko-fi](https://img.shields.io/badge/Support%20me-on%20Ko--fi-F16061?style=flat&logo=ko-fi&logoColor=white)](https://ko-fi.com/matu09)