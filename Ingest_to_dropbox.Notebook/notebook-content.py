# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3dbb6804-e902-4cdd-88e5-8a95e6986c24",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "fe4aac4e-7569-4ec8-b8f2-0200e2c8ff25",
# META       "known_lakehouses": [
# META         {
# META           "id": "3dbb6804-e902-4cdd-88e5-8a95e6986c24"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

import json
import requests
from pyspark.sql import *

print("Querying and aggregating dataset from Silver Lakehouse...")
df_spark = spark.sql("""
    SELECT 
        CAST(pickup_datetime AS DATE) AS DateKey, 
        pickup_hour AS HourOfDay,
        pickup_zone,
        COUNT(*) AS totaltrips, 
        CAST(SUM(total_amount) AS DOUBLE) AS totalrevenue
    FROM LH_silver.taxitrip_data_silver
    WHERE pickup_datetime BETWEEN '2023-09-01' AND '2023-09-30'
      AND pickup_datetime IS NOT NULL
      AND pickup_zone IS NOT NULL
    GROUP BY 
        CAST(pickup_datetime AS DATE),
        pickup_hour,
        pickup_zone
    ORDER BY DateKey ASC, HourOfDay ASC
""")

df_limited = df_spark.limit(100)

print("Serializing to JSON format...")

data_payload = []
for row in df_limited.collect():
    row_dict = row.asDict()
    if row_dict['DateKey'] is not None:
        row_dict['DateKey'] = str(row_dict['DateKey']) 
    data_payload.append(row_dict)

json_string = json.dumps(data_payload, indent=4)

FILE_NAME = "taxitrip_data_2023_09.json"
DROPBOX_ACCESS_TOKEN = "sl.u.AGgn968Gty_vw1EFXb6FjNnvroY2_-wvHDwsYPTWFQE7JNlQ1zgUxZn2kK3Xcogstg897GS0ySsknppb_4UXiYilE0oVgFJhaTYmDmzfKLG--lNqduOl_y7j5ZVAm6xVY7bwWPZsRaEvnvCz60Qefin_gSfn_7ULAcAiBiPBhMbWLfcvQ3PUBwN6OOCAoGNNfpbhPAwEaBBfOILRFv5wjtXa4X5uV5Y3HtcF8MVv7XHSvtq4vELFm8VrKQGggmUT8SHSho0XMSDYkhX4aAYo6gkERkammHl9VW68P3KTcxgn6X5igcJcXOIVXp3tu_ctnb4qFqTK1LvVNL2UOKuZV3c5gtvqN-zZxS6MU9_cGFQaoR7vzL-sY7Mk4DLlNQA2F3_mCBVp_i68ErB_3Lqa-G4mjxDpmPt4Op-O1IpXTtd4foMymziQh2a1nobsHjaMbyY4Ry2lV64ZIaKg5gBSkGCJAOulZ6Qya2Ta7NPVzJmTayDga6XrmwhzHt88B2sS_MRYhnZWzB7eUbYZnNPrUk8i5qXpV5T2dHeycEn4DYUVnAWv8dA5qKIV0sMXjqHAwtle6N9ddBFdXt_BD2AmCRWyiYtqzcfPvOLxo4KqLQE0qU5-ENHbw3GqYtaCfCRTGnhTeRPO59vqwFzC3wynA6cVsfwKDHhi6B1BXvoxhqqgyBcQ_48tto02IRiWyB1dcSsblBs8PC6ZYoBTapmTFNK-wAjF2aSN-kCDs6E1pyuZmLzzWlmhmGHvSc2eWqtz1L8meGQKPu5kjHkmGY4jZKN_hwfqJmPJouTb2IVJiguYR_eRi1gR0KWOQ3zPS4k5ux3gTsdabOSjQ5izY4_HN2PSrExvlqFvJzOeoaJmuMRd12tIZbrF5iHmrApFFmEdabBkvGVTYNwAuuvm2fId9Shay6vh-Hq8wshiKBIMgH7ScGOQSd52twTV13bxQFPWtijxc6LiaMZE0z92tqPA1RE2SsNiF8WFknEBNoF4h0MIECkEjI-QNLXNho5X8z5wtoKr4ROZZlHFF701PREYxeNisHukSuTBsNx6zMtFbdAj3RztMBeB7_9AxXIuNefldtWOENGEbHez1xScH5wPCTQI-OJjdCwVcQ4iSVzOcBMNT2zgO0lYc3U4F7VH-XccJiEHcaEcaPSavIo5YDplGpMKAwChqAwjKvcCN1zgYSRT6fMDEiAut0qT0_ctfHfnpW6E8zxO5YfmvGGOyONsGjrxD7LnLW20DisQIEy2r20Wz66fjD9Bo4mtJBCKSXUUFmaFKPDtGBCSVIhK8y38QJL8OzX3O3LEevyXrgNon3M2MaA6PF0Wa89G2bIQxFCcHqjQiCtZ5AlWeRwThnfMdE4JADxi1myLbzJHSvDBMNvRxYd5fWcDqSe-y3vCpMO61o7OtJtYNOaTBnzE8ofWbcyCif-92qL3mo67Bom416avujjSvp8t7VQEWcp83OyPwK8"  # Update with your active token

headers = {
    "Authorization": f"Bearer {DROPBOX_ACCESS_TOKEN}",
    "Dropbox-API-Arg": json.dumps({
        "path": f"/Fabric_data/{FILE_NAME}",
        "mode": "overwrite",
        "mute": False
    }),
    "Content-Type": "application/octet-stream"
}

print(f"Uploading '{FILE_NAME}' to Dropbox")
response = requests.post(
    "https://content.dropboxapi.com/2/files/upload", 
    headers=headers, 
    data=json_string
)

if response.status_code == 200:
    print("Data uploaded to Dropbox.")
else:
    print(f"Upload failed with status code {response.status_code}:")
    print(response.text)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
