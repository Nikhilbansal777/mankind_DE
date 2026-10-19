import pandas as pd
import numpy as np

df = pd.DataFrame({"product": ["A","B","C"], "sales": [100,250,175]})
print("✅ Pandas works:", df["sales"].sum())
print("✅ Numpy works:", np.mean(df["sales"]))
print("✅ All good! Environment is ready.")
