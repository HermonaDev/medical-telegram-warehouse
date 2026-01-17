import pytest
import pandas as pd

def test_dataframe_not_empty():
    # Example: Check if your detection results CSV exists and has data
    try:
        df = pd.read_csv('data/yolo_results.csv')
        assert not df.empty
        assert 'confidence' in df.columns
    except FileNotFoundError:
        pytest.skip("Data file not generated yet")