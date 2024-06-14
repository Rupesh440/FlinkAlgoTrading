import pandas as pd

# Sample DataFrame
data = {
    'timestamp': [
        '2024-05-02T09:15:00+05:30',
        '2024-05-02T09:16:00+05:30',
        '2024-05-02T09:17:00+05:30',
        '2024-05-02T09:18:00+05:30',
        '2024-05-02T09:19:00+05:30'
    ],
    'open': [826.90, 830.50, 830.70, 832.85, 832.85],
    'high': [831.0, 831.2, 833.0, 833.4, 833.0],
    'low': [825.00, 829.40, 830.40, 830.75, 831.50],
    'close': [830.45, 830.60, 832.85, 832.85, 831.70],
    'volume': [302173, 220040, 191248, 282668, 153414],
    'symboltoken': ['3045', '3045', '3045', '3045', '3045']
}

df = pd.DataFrame(data)

# Convert the 'timestamp' column to datetime objects and set the timezone
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Convert the timezone to UTC
df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')

# Serialize back to ISO 8601 string
#df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S%z')

# Display the updated DataFrame
print(df)
