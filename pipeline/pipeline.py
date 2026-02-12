import sys
import pandas as pd

print("arguements", sys.argv)
print("Pipeline module loaded successfully.")

a = [1, 2, 3, 4, 5]

myvar = pd.Series(a, index=['a', 'b', 'c', 'd', 'e'])
print(myvar)

data = {
    'calories': [200, 500, 300],
    'duration': [30, 45, 25]
}

df = pd.DataFrame(data)
print(df.head())

df.to_parquet('output.parquet')