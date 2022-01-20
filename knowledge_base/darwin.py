from knowledge_base import config
from suds.client import Client
from suds.sax.element import Element

DarwinClient = Client

def darwin_connect() -> Client:
    client = Client(config.DARWIN_API_URL)

    typ_namespace = ('typ', 'http://thalesgroup.com/RTTI/2013-11-28/Token/types')
    header = Element('AccessToken', ns=typ_namespace)
    token_value = Element('TokenValue', ns=typ_namespace).setText(config.DARWIN_TOKEN)
    header.append(token_value)

    client.set_options(service='ldb', port='LDBServiceSoap')
    client.set_options(soapheaders=header)
    return client

def darwin_get_arrival_departure_board(client: Client, from_crs: str, to_crs: str):
    result = client.service.GetArrDepBoardWithDetails(
        numRows=10, crs=from_crs, filterCrs=to_crs, filterType='to')
    return result

