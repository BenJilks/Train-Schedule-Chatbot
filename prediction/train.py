import pandas as pd
import numpy as np
import datetime
import argparse
from pandas import DataFrame
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Input
from tensorflow.keras.losses import CategoricalCrossentropy

def date_str_to_float(date_str: str) -> float:
    date = datetime.datetime.fromisoformat(date_str).date()
    date_int = date.year << 16 | date.month << 8 | date.day
    return float(date_int)

def time_str_to_float(time_str: str) -> float:
    time = datetime.datetime.strptime(time_str, '%H:%M:%S').time()
    time_int = time.hour << 8 | time.minute
    return float(time_int)

def crs_to_float(crs: str) -> float:
    return float(ord(crs[0]) << 16 | ord(crs[1]) << 8 | ord(crs[1]))

def row_string_to_float(row):
    row.toc = float(ord(row.toc[0]) << 8 | ord(row.toc[1]))
    row.from_crs = crs_to_float(row.from_crs)
    row.to_crs = crs_to_float(row.to_crs)
    row.date = date_str_to_float(row.date)
    row.departure_time = time_str_to_float(row.departure_time)
    row.arrival_time = time_str_to_float(row.arrival_time)
    row.late_0 = float(row.late_0)
    row.late_5 = float(row.late_5)
    row.late_10 = float(row.late_10)
    row.late_30 = float(row.late_30)
    row.was_late_0 = float(row.was_late_0)
    row.was_late_5 = float(row.was_late_5)
    row.was_late_10 = float(row.was_late_10)
    row.was_late_30 = float(row.was_late_30)
    return row

def load_training_data(file_path: str) -> tuple[np.ndarray, np.ndarray]:
    training_data = pd.read_csv(
        file_path,
        names = [
            'toc', 'from_crs', 'to_crs',
            'date', 'departure_time', 'arrival_time',
            'late_0', 'late_5', 'late_10', 'late_30',
            'was_late_0', 'was_late_5', 'was_late_10', 'was_late_30'])
    assert isinstance(training_data, DataFrame)

    training_data = training_data.transform(row_string_to_float, axis=1)
    training_data = training_data.transform(np.float32)
    training_data = training_data.drop(['toc', 'from_crs', 'to_crs', 'date', 'departure_time', 'arrival_time'], axis=1)
    assert not training_data is None

    output_columns = ['was_late_0', 'was_late_5', 'was_late_10', 'was_late_30']
    labels = training_data[output_columns].copy()
    features = training_data.drop(output_columns, axis=1)
    return np.array(features), np.array(labels)

def main():
    parser = argparse.ArgumentParser(description='Collect training data')
    parser.add_argument('--training-data', '-d', help='Path of training data', required=True)
    parser.add_argument('--epochs', '-e', help='Epocks to use for training', required=True)
    args = parser.parse_args()

    train, test = load_training_data(args.training_data)
    model = Sequential([
        Input(shape=(4,)),
        Dense(100),
        Dense(4)])
    model.summary()

    loss_fn = CategoricalCrossentropy(from_logits=True)
    model.compile(optimizer='adam',
                  loss=loss_fn,
                  metrics=['accuracy'])
    
    model.fit(train, test, epochs=int(args.epochs))
    model.evaluate(train, test, verbose=2)
    model.save('delays.model')

if __name__ == '__main__':
    main()

