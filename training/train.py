import os
import joblib
import statsmodels.api as sm
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
import pandas as pd

import matplotlib.pyplot as plt

BASE_OUTPUT_PATH = os.path.dirname(os.path.abspath(__file__)) + "/../models"
BASE_DATA_FILE_PATH = os.path.dirname(os.path.abspath(__file__)) + "/../data"

def train_model(data, hyperparams):
    model = sm.OLS(data["train"]["y"], data["train"]["X"]).fit()
    preds = model.predict(data["test"]["X"])
    print(str(model.summary()))
    return model

def main(data_file_name, model_name):
    print("Running train.py")

    hyperparams = []

    data_file_path = os.path.join(BASE_DATA_FILE_PATH, data_file_name)
    # Get the dataframe 
    df = pd.read_csv(data_file_path) 
    if (df is not None):
        X = df[["price", "lag"]]  # Feature Matrix
        y = df["minPrice"]  # Target Variable
    else:
        e = ("No dataframe provided")
        print(e)
        raise Exception(e)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=0)
    data = {"train": {"X": X_train, "y": y_train},
            "test": {"X": X_test, "y": y_test}}

    model = train_model(data, hyperparams)

    # Save model file to disk
    os.makedirs(BASE_OUTPUT_PATH, exist_ok=True)
    model_output_path = os.path.join(BASE_OUTPUT_PATH, model_name)
    joblib.dump(value=model, filename=model_output_path)

    # x_pred1 = (124.25, 1)
    # y_pred1 = model.predict(x_pred1)
    # print(f"Ejemplo ({x_pred1}) = {y_pred1}")

    x_pred1 = X_test["price"]
    y_pred1 = y_test

if __name__ == '__main__':
    main('test_BCN_MAD_processed.csv', 'BCN-MAD.bin')
    main('test_MAD_BCN_processed.csv', 'MAD-BCN.bin')
    main('test_LCG_MAD_processed.csv', 'LCG-MAD.bin')
    main('test_MAD_LCG_processed.csv', 'MAD-LCG.bin')