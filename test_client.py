from blsqpy.dhis2_client import Dhis2Client

dhis2=Dhis2Client("https://admin:district@play.dhis2.org/2.32.2")
print(dhis2.data_elements_structure())
