#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'Stefan Jansen'

import pandas as pd
import numpy as np
from os.path import join
from multiprocessing import Pool
import boto3
from io import StringIO
from geocode_api import GeoEncoder
import warnings

API_BATCH_SIZE = 1000
ADDRESS_COLS = ['id', 'address', 'city', 'state', 'postalcode']


def save_to_s3(args):
    data, batch, i = args
    print(batch, i)
    BUCKET = 'direct-mailing'
    PATH = 'encoded'
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(BUCKET, join(PATH, 'batch_{}.csv'.format(batch, i))).put(Body=csv_buffer.getvalue())


def save_local(args):
    data, batch = args
    print(batch)
    data.to_csv(join('encoded', 'batch_{}.csv'.format(batch)), index=False)


def match_addresses(data, batch):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        encoder = GeoEncoder(data)
        results = encoder.parse_results()
    return data.merge(results, on='id', how='left'), batch


def main():
    addresses = pd.read_csv('addresses.csv').reset_index(drop=True)

    with Pool() as pool:
        results = []
        for batch, (_, df) in enumerate(addresses.groupby(np.arange(len(addresses)) // API_BATCH_SIZE)):
            r = pool.apply_async(match_addresses, (df, batch), callback=save_local)
            results.append(r)
        for r in results:
            r.wait()


if __name__ == '__main__':
    main()
