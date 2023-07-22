def get_aircraftdb():
    '''
    Fungsi ini digunakan untuk mendownload data aircraftDB berformat CSV
    pada website OpenSky-Network.
    '''
    import requests
    import pandas as pd

    url = 'https://opensky-network.org/datasets/metadata/aircraftDatabase.csv'
    save_path = '/home/fatir/datalake-flight/aircraftDatabase.csv'

    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(save_path, 'w') as file:
        file.write(response.text)
    print("aircraftDB File downloaded successfully.")

    df = pd.read_csv('/home/fatir/datalake-flight/aircraftDatabase.csv')
    df.to_csv('/home/fatir/datalake-flight/aircraftDatabase.csv', index=False)

def get_aircrafttypes():
    '''
    Fungsi ini digunakan untuk mendownload data AircraftTypes berformat CSV
    pada website OpenSky-Network.
    '''
    import requests
    import pandas as pd

    url = 'https://opensky-network.org/datasets/metadata/doc8643AircraftTypes.csv'
    save_path = '/home/fatir/datalake-flight/doc8643AircraftTypes.csv'

    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(save_path, 'w') as file:
        file.write(response.text)
    print("AircraftTypes File downloaded successfully.")

    # df = pd.read_csv('/home/fatir/datalake-flight/doc8643AircraftTypes.csv')
    # df.to_csv('/home/fatir/datalake-flight/doc8643AircraftTypes.csv', index=False)

def get_airport():
    '''
    Fungsi ini digunakan untuk mendownload data Airport berformat CSV
    pada website OpenSky-Network.
    '''
    import requests
    import pandas as pd

    url = 'https://raw.githubusercontent.com/fajartirtayasa/Flights-Data-Pipeline/main/OpenSky_airport_data.csv'
    save_path = '/home/fatir/datalake-flight/Airport.csv'

    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(save_path, 'w') as file:
        file.write(response.text)
    print("Airport File downloaded successfully.")

    df = pd.read_csv('/home/fatir/datalake-flight/Airport.csv')
    df.to_csv('/home/fatir/datalake-flight/Airport.csv', index=False)


# get_aircraftdb()
# get_aircrafttypes()
# get_airport()