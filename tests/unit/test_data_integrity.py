import pytest
import os
import pandas as pd

def test_yolo_results_schema():
    """Verify the YOLO output has the expected columns for the warehouse."""
    path = "data/yolo_results.csv"
    if not os.path.exists(path):
        pytest.skip("Detection results not found, skipping integrity check.")
    
    df = pd.read_csv(path)
    expected_cols = ['channel', 'label', 'confidence']
    for col in expected_cols:
        assert col in df.columns, f"Missing required column: {col}"

def test_dbt_project_structure():
    """Verify dbt project is correctly configured."""
    assert os.path.exists("kara_dbt/dbt_project.yml")
    assert os.path.exists("kara_dbt/models/marts/fct_messages.sql")