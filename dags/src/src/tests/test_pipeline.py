"""
Pytest Suite for Data Processing and Anonymization
"""
import pytest
import pandas as pd
import hashlib

def anonymize_row(customer_name: str) -> str:
    return hashlib.sha256(f"{customer_name}_DHL_SALT_2026".encode()).hexdigest()[:16]

def test_anonymization_length_and_determinism():
    raw_name = "Akpevwe Peters"
    hashed_1 = anonymize_row(raw_name)
    hashed_2 = anonymize_row(raw_name)
    
    assert len(hashed_1) == 16
    assert hashed_1 == hashed_2
    assert raw_name not in hashed_1

def test_transit_duration_calculation():
    planned = 24.0
    actual = 28.5
    delay_hours = actual - planned
    assert delay_hours == 4.5
