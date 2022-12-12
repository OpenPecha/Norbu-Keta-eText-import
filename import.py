import pandas as pd

def read_csv(path):
    df = pd.read_csv(path,header=None)
    df = df.sort_values([3,2])


if __name__ == "__main__":
    read_csv("demo.csv")