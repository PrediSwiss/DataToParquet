import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import pyarrow as pa
from datetime import datetime, timedelta
from dateutil import rrule
import xml.etree.ElementTree as ET
from google.cloud import storage

bucket_name = "prediswiss-parquet-data"
bucket_row = "prediswiss-raw-data"

def main():
    #table = pq.read_table('test.parquet')
    #df = table.to_pandas()
    #print(df.groupby(["publication_date"]).count())
    #print(df[df.publication_date == "2023-05-03T19:34:18.013611Z"])
    storage_client = storage.Client(project="prediswiss")

    try:
        bucketParquet = storage_client.get_bucket(bucket_name)
    except:
        bucketParquet = create_bucket(bucket_name, storage_client)

    try:
        bucketRaw = storage_client.get_bucket(bucket_row)
    except:
        raise RawDataException
    
    now = datetime.now()
    hourEarlier = now - timedelta(hours=1)   
    
    dataframe = pd.DataFrame()

    for dt in rrule.rrule(rrule.MINUTELY, dtstart=hourEarlier, until=now):
        blob = bucketRaw.get_blob(dt.strftime("%Y-%m-%d/H-%M"))
        data = blob.download_as_text()

        namespaces = {
            'ns0': 'http://schemas.xmlsoap.org/soap/envelope/',
            'ns1': 'http://datex2.eu/schema/2/2_0',
        }

        dom = ET.fromstring(data)

        publicationDate = dom.find(
            './ns0:Body'
            '/ns1:d2LogicalModel'
            '/ns1:payloadPublication'
            '/ns1:publicationTime',
            namespaces
        ).text

        compteurs = dom.findall(
            './ns0:Body'
            '/ns1:d2LogicalModel'
            '/ns1:payloadPublication'
            '/ns1:siteMeasurements',
            namespaces
        )

        flowTmp = [(compteur.find(
            './ns1:measuredValue[@index="1"]'
            '/ns1:measuredValue'
            '/ns1:basicData'
            '/ns1:vehicleFlow'
            '/ns1:vehicleFlowRate',
            namespaces
        ), compteur.find(
            './ns1:measuredValue[@index="11"]'
            '/ns1:measuredValue'
            '/ns1:basicData'
            '/ns1:vehicleFlow'
            '/ns1:vehicleFlowRate',
            namespaces
        ), compteur.find(
            './ns1:measuredValue[@index="21"]'
            '/ns1:measuredValue'
            '/ns1:basicData'
            '/ns1:vehicleFlow'
            '/ns1:vehicleFlowRate',
            namespaces
        )) for compteur in compteurs]

        speedTmp = [(compteur.find(
            './ns1:measuredValue[@index="2"]'
            '/ns1:measuredValue'
            '/ns1:basicData'
            '/ns1:averageVehicleSpeed'
            '/ns1:speed',
            namespaces
        ), compteur.find(
            './ns1:measuredValue[@index="12"]'
            '/ns1:measuredValue'
            '/ns1:basicData'
            '/ns1:averageVehicleSpeed'
            '/ns1:speed',
            namespaces
        ), compteur.find(
            './ns1:measuredValue[@index="22"]'
            '/ns1:measuredValue'
            '/ns1:basicData'
            '/ns1:averageVehicleSpeed'
            '/ns1:speed',
            namespaces
        )) for compteur in compteurs]

        speed = []
        for sp in speedTmp:
            speed.append((None if sp[0] == None else sp[0].text, None if sp[1] == None else sp[1].text, None if sp[2] == None else sp[2].text))

        flow = []
        for fl in flowTmp:
            flow.append((None if fl[0] == None else fl[0].text, None if fl[1] == None else fl[1].text, None if fl[2] == None else fl[2].text))

        compteursId = [comp.find('ns1:measurementSiteReference', namespaces).get("id") for comp in compteurs]

        columns = ["publication_date", "id", "flow_1", "flow_11", "flow_21", "speed_2", "speed_12", "speed_22"]

        data = [(publicationDate, compteursId[i], flow[i][0], flow[i][1], flow[i][2], speed[i][0], speed[i][1], speed[i][2]) for i in range(len(compteursId)) ]
        dataframe = pd.concat([pd.DataFrame(data=data, columns=columns), dataframe])
    
    table = pa.Table.from_pandas(dataframe)
    pq.write_to_dataset(table, root_path="test.parquet")


    #{heure : {CH:0542.05 : [120,0,23], CH:0026.02 : [23, 1, 123]}},{heure : {CH:0232.05 : [10,10,53], CH:0223.01 : [65, 12, 73]}}
    #table = pa.Table.from_pandas(df)
    #pq.write_table(table, 'example.parquet')

def create_bucket(name, client: storage.Client):    
    bucket = client.create_bucket(name, location="us-east1")
    print(f"Bucket {name} created")
    return bucket

def create_blob(root_bucket: storage.Bucket, destination_name, data_type, data):
    blob = root_bucket.blob(destination_name)
    generation_match_precondition = 0
    blob.upload_from_string(data, data_type, if_generation_match=generation_match_precondition)
    print("file created")

if __name__ == "__main__":
    main()

class RawDataException(Exception):
    "Raised when ubucket of raw data doesn't exist"
    pass