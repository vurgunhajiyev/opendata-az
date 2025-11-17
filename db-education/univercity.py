import requests
import pandas as pd

# 1. CKAN dataset API
dataset_id = "69615758-35a0-4e0d-bec2-1a3d4a13d352"
url = f"https://admin.opendata.az/api/3/action/package_show?id={dataset_id}"

response = requests.get(url)
data = response.json()

# 2. Resource metadata
resource = data["result"]["resources"][0]
csv_url = resource["url"]

print("CSV URL:", csv_url)

# 3. Read CSV
df = pd.read_csv(csv_url)
print(df.head())

# 4. Save locally
df.to_csv("universities.csv", index=False)
