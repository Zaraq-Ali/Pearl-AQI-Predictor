from datetime import datetime, timedelta
from generate_features import fetch_data, compute_features, save_to_feature_store

start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 7, 5 )

for i in range((end_date - start_date).days + 1):
    date = (start_date + timedelta(days=i)).strftime('%Y-%m-%d')
    raw = fetch_data("Delhi", date)
    df = compute_features(raw)
    save_to_feature_store(df, date)
