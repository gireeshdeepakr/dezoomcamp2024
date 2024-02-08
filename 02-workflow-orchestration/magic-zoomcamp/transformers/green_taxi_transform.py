if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    #Remove rows where the passenger count is equal to 0 and the trip distance is equal to zero.
    clean_data = data[(data['passenger_count'] != 0) & (data['trip_distance'] != 0)]
    
    #Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date
    clean_data['lpep_pickup_date'] = clean_data['lpep_pickup_datetime'].dt.date
    
    #Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id
    clean_data.columns = (clean_data.columns
              .str.replace(r'(?<=[a-z])(?=[A-Z])', '_', regex=True)
              .str.lower())
    return clean_data


@test
def test_output(output, *args) -> None:
#def test_output(data, *args)
    assert all(value in [1, 2] for value in output['vendor_id']), "Vendor IDs must be 1 or 2."
    assert (output['passenger_count'] > 0).all(), "Passenger count must be greater than 0."
    assert (output['trip_distance'] > 0).all(), "Trip distance must be greater than 0."
    