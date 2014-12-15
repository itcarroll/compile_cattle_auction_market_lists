from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL as create_url
from sqlalchemy.orm import sessionmaker
from os.path import expanduser
import sys


geoname_query_field = ['geonameId', 'adminCode1', 'adminCode2']


def create_session(database=None, port=None, check=True, echo=False):

    connection = create_url(
        drivername='mysql+mysqlconnector',
        host='127.0.0.1',
        )
    if database:
        connection.database = database
    if port:
        connection.port = port

    # Confirm database
    if check:
        really = input('Connection string: {}\nIs that correct? yes/(no) '.format(connection))
        if really != 'yes':
            print('Okay, good thing we checked. Bye.')
            sys.exit()

    home = expanduser('~')
    engine = create_engine(connection, connect_args={'option_files': home + '/.my.cnf'}, echo=echo)
    Session = sessionmaker(bind=engine)

    return Session()


state_abbr = {
    'AK': '02',
    'AL': '01',
    'AR': '05',
    'AS': '60',
    'AZ': '04',
    'CA': '06',
    'CO': '08',
    'CT': '09',
    'DC': '11',
    'DE': '10',
    'FL': '12',
    'GA': '13',
    'GU': '66',
    'HI': '15',
    'IA': '19',
    'ID': '16',
    'IL': '17',
    'IN': '18',
    'KS': '20',
    'KY': '21',
    'LA': '22',
    'MA': '25',
    'MD': '24',
    'ME': '23',
    'MI': '26',
    'MN': '27',
    'MO': '29',
    'MS': '28',
    'MT': '30',
    'NC': '37',
    'ND': '38',
    'NE': '31',
    'NH': '33',
    'NJ': '34',
    'NM': '35',
    'NV': '32',
    'NY': '36',
    'OH': '39',
    'OK': '40',
    'OR': '41',
    'PA': '42',
    'PR': '72',
    'RI': '44',
    'SC': '45',
    'SD': '46',
    'TN': '47',
    'TX': '48',
    'UT': '49',
    'VA': '51',
    'VI': '78',
    'VT': '50',
    'WA': '53',
    'WI': '55',
    'WV': '54',
    'WY': '56',
    }
