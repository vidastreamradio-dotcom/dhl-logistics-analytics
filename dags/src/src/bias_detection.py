"""
Bias Detection Suite checking representation equity across delivery zones
"""
import pandas as pd

def evaluate_representation_bias(df: pd.DataFrame, group_col: str = 'origin_hub') -> dict:
    distribution = df[group_col].value_counts(normalize=True).to_dict()
    disparity_flag = False
    
    # Check disparity threshold
    for group, ratio in distribution.items():
        if ratio < 0.15:  # Alert if any region falls below 15% sample representation
            print(f"[WARNING] Disparity Detected! Group '{group}' is underrepresented: {ratio:.2%}")
            disparity_flag = True
            
    return {
        "distribution": distribution,
        "disparity_detected": disparity_flag
    }

if __name__ == "__main__":
    data = pd.read_csv("data/processed_shipment_data.csv")
    results = evaluate_representation_bias(data)
    print("Representation Assessment Results:", results)
