import re
import json
from time import sleep
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from sqlalchemy import or_, not_
from sqlalchemy.orm import aliased
from db_class import Base, Premises, Geoname
from db_class import AMS, APHIS, GIPSA, LMA, Market
from db_util import state_abbr, geoname_query_field

FT_NAME_THRESHOLD = 5
FT_ADDRESS_THRESHOLD = 4
FT_NAME_ONLY_THRESHOLD = 10

mapquestapi_key = ""  # Required user input.
geonames_user = ""  # Required user input.


def assign_premises(session):
    """Assign a premises id to each market, mapping duplicates to the same premises."""

    def next_match(session, market, market_chain):
        """Search for matching markets by finding the next closest match among
        markets not already in the current chain of matches."""
        
        result = None
        exclude = set(this_market.pk for this_market in market_chain)

        # During import, rows with multiple versions of an attribute were split.
        # These are certainly duplicates.
        row = getattr(market, 'row', None)
        ThisMarket = type(market)
        if row:
           result = session.query(ThisMarket
               ).filter(not_(ThisMarket.pk.in_(exclude))
               ).filter_by(row=row).first()

        def get_query(exclude, city=True):
            # Search against markets beyond the existing chain of matches
            # or anything else in exclude, added from non-null mis-matches.
            query = session.query(Market).filter(not_(Market.pk.in_(exclude)))
            # State filter applies to all remaining matches.
            query = query.filter_by(state=market.state)
            if city:
                # City filter often applies
                query = query.filter_by(city=market.city)
            return query

        def also_exclude(exclude):
            SameMarket = aliased(Market)
            query = session.query(SameMarket).join(Market, Market.premises_id==SameMarket.premises_id)
            return query.filter(Market.pk.in_(exclude))

        # Gather null features, as needed and if available, from match_chain
        c = {k: None for k in ['name', 'address', 'po']}
        for k in c:
            c[k] = [getattr(this_market, k, None) for this_market in market_chain]
            c[k] = [v for v in c[k] if v]

        # The following matches are all valid, but their order implies precedence.
        if not result and c['po'] and market.city:
            query = get_query(exclude)
            po_filter = [Market.po==v for v in c['po']]
            next_query = query.filter(or_(*po_filter))
            result = next_query.first()
            next_query = query.filter(Market.po!=None)
            exclude |= set(r.pk for r in next_query)
            exclude |= set(r.pk for r in also_exclude(exclude))


        if not result and c['address'] and market.city:
            query = get_query(exclude)
            for address in c['address']:
                full_text = Market.address.match(address)
                next_query = query.filter(full_text > FT_ADDRESS_THRESHOLD).order_by(full_text.desc())
                result = next_query.first()
                if result:
                    break
            next_query = query.filter(Market.address!=None)
            exclude |= set(r.pk for r in next_query)
            exclude |= set(r.pk for r in also_exclude(exclude))

        if not result and c['name'] and market.city:
            query = get_query(exclude)
            for name in c['name']:
                full_text = Market.name.match(name)
                next_query = query.filter(full_text > FT_NAME_THRESHOLD).order_by(full_text.desc())
                result = next_query.first()
                if result:
                    break
            next_query = query.filter(Market.name!=None)
            exclude |= set(r.pk for r in next_query)
            exclude |= set(r.pk for r in also_exclude(exclude))

        if not result and not any(c.values()) and market.city:
            query = get_query(exclude)
            result = query.first()

        if not result and c['name']:
            query = get_query(exclude, city=False)
            next_query = None
            if c['address'] and not c['po']:
                next_query = query.filter(Market.address==None, Market.po!=None)
            elif c['po'] and not c['address']:
                next_query = query.filter(Market.address!=None, Market.po==None)
            if next_query:
                for name in c['name']:
                    full_text = Market.name.match(name)
                    next_query = next_query.filter(full_text > FT_NAME_ONLY_THRESHOLD).order_by(full_text.desc())
                    result = next_query.first()
                    if result:
                        break

        return result

    market = session.query(Market).filter_by(premises_id=None).first()
    while market:
        match_chain = []
        repeat = True
        while repeat:
            match_chain.append(market)
            match = next_match(session, market, match_chain)
            premises = getattr(match, 'premises', None)
            if match and premises:
                repeat = False
            elif not match:
                repeat = False
            else:
                market = match

        if not premises:
            premises = Premises()
        for this_match in match_chain:
            this_match.premises = premises

        session.commit()
        market = session.query(Market).filter_by(premises_id=None).first()


def assign_geoname(session):
    """For each premises, assign a county that contains the given address, city or zip code."""

    def debbreviate(location):

        search = [
            {'pattern': r'(^|\b)St\.? ', 'sub':'Saint '},
            {'pattern': r'(^|\b)Mt\.? ', 'sub':'Mount '},
            {'pattern': r'(^|\b)Ft\.? ', 'sub':'Fort '},
            {'pattern': r'(^|\b)N\.? ', 'sub':'North '},
            {'pattern': r'(^|\b)S\.? ', 'sub':'South '},
            {'pattern': r'(^|\b)Mc ', 'sub':'Mc'},
            {'pattern': r'(^|\b)Sprgs', 'sub':'Springs'},
            ]

        city = location['city']
        for this_search in search:
            city = re.sub(this_search['pattern'], this_search['sub'], city, flags=re.IGNORECASE)

        if city==location['city']:
            change = False
        else:
            location['city'] = city
            change = True

        return change

    def location_search(location, most_fuzzy=0):

        geoname = []

        if location.get('address'):
            street = {k: v for k, v in location.items() if k in ['address', 'city', 'state']}
            match, mapquest = query_open_mapquest(street, ['ADDRESS', 'STREET'])
            if len(mapquest)==1:
                fuzzy = None
                lat_lng = mapquest[0]['latLng']
                geoname = query_geoname_reverse(lat_lng)

        if not geoname and not location.get('city'):
            if location.get('zip'):
                zip = {k: v for k, v in location.items() if k in ['zip', 'state']}
                match, mapquest = query_open_mapquest(zip, ['ZIP'])
            elif location.get('address'):
                street = {k: v for k, v in location.items() if k in ['address', 'city', 'state']}
                match, mapquest = query_open_mapquest(street, ['ADDRESS', 'STREET'])
            if len(mapquest)==1:
                location['city'] = mapquest[0]['adminArea5']
            else:
                print("Multiple results returned for premises {}.".format(premises.id))

        if not geoname:
            fuzzy = -1
            while fuzzy <= most_fuzzy:
                fuzzy += 1
                geoname = query_geoname(location, fuzzy)
                if geoname:
                    break

        if not geoname:
            fuzzy = None
            success = debbreviate(location)
            if success:
                geoname = query_geoname(location, 0)

        if not geoname and location.get('zip'):
            fuzzy = 6
            geoname = query_geoname(location, fuzzy)
            zip = {k: v for k, v in location.items() if k in ['zip']}
            match, mapquest = query_open_mapquest(zip, ['ZIP'], geoname)
            if match:
                geoname = [match]
            elif len(mapquest)==1:
                fuzzy = None
                lat_lng = mapquest[0]['latLng']
                geoname = query_geoname_reverse(lat_lng)

        if len(geoname) > 1:
            match = False
            if not match and location.get('zip'):
                zip = {k: v for k, v in location.items() if k in ['zip']}
                match, mapquest = query_open_mapquest(zip, ['ZIP'], geoname)
            if not match and location.get('address'):
                street = {k: v for k, v in location.items() if k in ['address', 'city', 'state']}
                match, mapquest = query_open_mapquest(street, ['ADDRESS', 'STREET'], geoname)
            if not match and location.get('address'):
                street = {k: v for k, v in location.items() if k in ['address', 'state']}
                match, mapquest = query_open_mapquest(street, ['ADDRESS', 'STREET'], geoname)
            if not match:
                print("No match obtained between Geonames and Nominatim for premises {}.".format(premises.id))
            else:
                geoname = [match]

        return geoname, fuzzy

    def query_geoname(location, fuzzy=0):

        base_url = 'http://api.geonames.org/searchJSON?'
        param = {
            'name_equals': location['city'],
            'adminCode1': location['state'],
            'fuzzy': "{:.1f}".format(0.1*(10 - fuzzy)),
            'style': 'full',
            'username': geonames_user,
            'featureClass': 'P',
            'continentCode': 'NA',
            }

        query = {k: v for k, v in param.items() if v}
        geoname_url = base_url + urlencode(query)

        request = Request(geoname_url)
        sleep(0.1)
        with urlopen(request) as io:
            response = json.loads(io.read().decode())
        if 'geonames' in response:
            geoname = response['geonames']
        geoname = [
            this_geoname for this_geoname in geoname
            if this_geoname.get('adminCode2') and (this_geoname['adminCode1'] in state_abbr.keys())
            ]

        return geoname

    def query_geoname_reverse(lat_lng):

        base_url = 'http://api.geonames.org/findNearbyPlaceNameJSON?'
        param = {
            'lat': lat_lng['lat'],
            'lng': lat_lng['lng'],
            'style': 'full',
            'username': 'roundup',
            }

        query = {k: v for k, v in param.items() if v}
        geoname_url = base_url + urlencode(query)

        request = Request(geoname_url)
        sleep(0.1)
        with urlopen(request) as io:
            response = json.loads(io.read().decode())
        if 'geonames' in response:
            geoname = response['geonames']
        geoname = [
            this_geoname for this_geoname in geoname
            if this_geoname.get('adminCode2') and (this_geoname['adminCode1'] in state_abbr.keys())
            ]

        return geoname

    def query_open_mapquest(location, geocodeQuality, geoname=[]):

        base_url = 'http://open.mapquestapi.com/geocoding/v1/address?key=' + mapquestapi_key
        param = {
            'street': location.get('address'),
            'city': location.get('city'),
            'state': location.get('state'),
            'postalCode': location.get('zip'),
            'country': 'US',
            }

        query = {k: v for k, v in param.items() if v}
        url = base_url + urlencode(query)

        request = Request(url)
        with urlopen(request) as io:
            response = json.loads(io.read().decode())
        mapquest = [
            res for res in response['results'][0]['locations']
            if res['geocodeQuality'] in geocodeQuality
            ]

        match = None
        for this_mapquest in mapquest:
            match = next((
                this_geoname for this_geoname in geoname
                if this_mapquest['adminArea4'] in this_geoname['adminName2']
                ), None)
            if match:
                break

        return match, mapquest

    def get_geoname(session, location):

        geoname, fuzzy = location_search(location, most_fuzzy=2)

        for idx, this_geoname in enumerate(geoname):
            result = session.query(Geoname).filter_by(geonameId=this_geoname['geonameId']).first()
            if result:
                geoname[idx] = result
            else:
                this_geoname = {k: v for k, v in this_geoname.items() if k in geoname_query_field and v}
                if fuzzy:
                    this_geoname['fuzzy'] = "{:.1f}".format(0.1*(10 - fuzzy))
                this_geoname = Geoname(**this_geoname)
                session.add(this_geoname)
                geoname[idx] = this_geoname

        return geoname

    outer_query = session.query(Premises).join(Market
        ).filter(Premises.geoname_id==None
        ).group_by(Premises.id
        )
    for premises in outer_query:

        query = session.query(Market).filter_by(premises=premises)
        query = query.filter(or_(
            Market.address!=None,
            Market.city!=None,
            Market.zip!=None,
            ))

        # Prefer city, state associated with non-null address, then zip
        query = query.order_by(Market.po, Market.address.desc())

        # Locate each element of location
        location = []
        for result in query:
            location.append(
                {k: getattr(result, k) for k in ['address', 'city', 'state', 'zip']}
                )
        geoname = []
        for this_location in location:
            geoname = get_geoname(session, this_location)
            if len(geoname)==1:
                break

        if len(geoname)==0:
            print("No geoname for premises {}.".format(premises.id))
            session.rollback()
        elif len(geoname)>1:
            print("Multiple geonames for premises {}.".format(premises.id))
            session.rollback()
        else:
            geoname = geoname[0]
            if not geoname.premises_id:
                geoname.premises = premises
            if premises.geoname_id:
                print('Premises has already been located!?')
                session.rollback()
            else:
                premises.geoname = geoname
                session.commit()
