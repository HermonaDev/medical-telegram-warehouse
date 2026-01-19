from dagster import asset, Definitions
import subprocess
import os
import sys

PYTHON_EXE = sys.executable

# Add the project root to the environment variables
# This allows scripts to find modules like 'scripts.logger_config'
custom_env = os.environ.copy()
custom_env["PYTHONPATH"] = os.getcwd()

@asset(group_name="extraction")
def telegram_scraper():
    """Step 1: Scrape raw messages and images from Telegram."""
    # We pass the custom_env here
    subprocess.run([PYTHON_EXE, "scripts/scraper.py"], env=custom_env, check=True)

@asset(deps=[telegram_scraper], group_name="detection")
def yolo_object_detection():
    """Step 2: Run YOLO model on downloaded images."""
    subprocess.run([PYTHON_EXE, "scripts/yolo_detection.py"], env=custom_env, check=True)

@asset(deps=[yolo_object_detection], group_name="transformation")
def dbt_warehouse_models():
    """Step 3: Run dbt to transform raw data."""
    dbt_path = os.path.join(os.getcwd(), "kara_dbt")
    # Note: dbt doesn't usually need the PYTHONPATH, but it doesn't hurt
    subprocess.run(["dbt", "run"], cwd=dbt_path, check=True)

defs = Definitions(
    assets=[telegram_scraper, yolo_object_detection, dbt_warehouse_models],
)