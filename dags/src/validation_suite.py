"""
Automated Data Validation Suite using Great Expectations
"""
import pandas as pd
import great_expectations as ge

def run_ge_validation_suite(file_path: str):
    df = ge.read_csv(file_path)
    
    # Assert minimum row count (5,000 records rule)
    res_count = df.expect_table_row_count_to_be_between(min_value=5000, max_value=1000000)
    assert res_count.success, "Data Validation Error: Row count below 5,000!"
    
    # Assert primary key uniqueness
    res_pk = df.expect_column_values_to_be_unique("shipment_id")
    assert res_pk.success, "Data Validation Error: Non-unique shipment IDs!"
    
    # Assert no nulls in critical operational columns
    for col in ["origin_hub", "destination_hub", "service_tier", "actual_transit_hours"]:
        res_null = df.expect_column_values_to_not_be_null(col)
        assert res_null.success, f"Data Validation Error: Nulls found in {col}!"
        
    # Assert service tier categorical values
    res_cat = df.expect_column_values_to_be_in_set("service_tier", ["Express", "Standard", "SameDay"])
    assert res_cat.success, "Data Validation Error: Invalid service tier detected!"
    
    print("All Great Expectations validation assertions passed successfully!")

if __name__ == "__main__":
    run_ge_validation_suite("data/processed_shipment_data.csv")
