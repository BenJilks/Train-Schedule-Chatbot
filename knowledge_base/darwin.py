from zeep.client import Client
from zeep.plugins import HistoryPlugin
from zeep import xsd
from dataclasses import dataclass

LDB_TOKEN = 'c77dcba0-aed9-426f-97b5-e52274822e42'
WSDL = 'http://lite.realtime.nationalrail.co.uk/OpenLDBWS/wsdl.aspx?ver=2017-10-01'

@dataclass
class Darwin:
    client: Client
    header: xsd.Element

def darwin_connect() -> Darwin:
    history = HistoryPlugin()
    client = Client(wsdl=WSDL, plugins=[history])

    header = xsd.Element(
        '{http://thalesgroup.com/RTTI/2013-11-28/Token/types}AccessToken',
        xsd.ComplexType([
            xsd.Element(
                '{http://thalesgroup.com/RTTI/2013-11-28/Token/types}TokenValue',
                xsd.String()),
        ])
    )

    header_value = header(TokenValue=LDB_TOKEN)
    return Darwin(client, header_value)

def darwin_get_arrival_departure_board(darwin: Darwin, crs_location: str):
    result = darwin.client.service.GetArrDepBoardWithDetails(
        numRows=10, crs=crs_location, _soapheaders=[darwin.header])
    return result

