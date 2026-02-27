import pandas as pd
import reverse_geocoder as rg
from sqlalchemy import create_engine

# 1. Define Functions at the top level
def parse_gps(lat, lon):
    try:
        return (float(lat), float(lon))
    except:
        return (0, 0) # Handle errors

# 2. Protect the execution logic
if __name__ == '__main__':
    # Connect to Database (Silver Layer)
    # Replace with your actual connection string
    db_connection_str = 'mysql+pymysql://root:@localhost/dw_silver'
    db_connection = create_engine(db_connection_str)

    print("Fetching GPS data...")
    # Read only unique GPS points to save processing time
    df = pd.read_sql("SELECT DISTINCT pos_gps_lat, pos_gps_long FROM telematics_tracking", db_connection)

    # Prepare Coordinates
    coordinates = df.apply(lambda row: parse_gps(row['pos_gps_lat'], row['pos_gps_long']), axis=1).tolist()

    # Perform Offline Reverse Geocoding
    print("Geocoding coordinates...")
    # Because this is now inside the main block, it can safely spawn processes!
    results = rg.search(coordinates) 

    # Map Results back to DataFrame
    df['country_code'] = [x['cc'] for x in results]
    df['country'] = [x['admin1'] for x in results] # Region/State

    # Write to a new Reference Table in Silver
    print("Writing to database...")
    df.to_sql('ref_gps_country', db_connection, if_exists='replace', index=False)

    print("Done! Created table 'ref_gps_country'.")