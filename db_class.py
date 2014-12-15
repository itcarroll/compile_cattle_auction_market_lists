from sqlalchemy import ForeignKey
from sqlalchemy import Column, Integer, String, Float, Numeric, Date, Text
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()

class Premises(Base):

    __tablename__ = 'premises'

    id = Column(Integer, primary_key=True)
    geoname_id = Column(None, ForeignKey('geoname.id'))

    geoname = relationship('Geoname', foreign_keys=geoname_id)


class Geoname(Base):

    __tablename__ = 'geoname'

    id = Column(Integer, primary_key=True)
    premises_id = Column(None, ForeignKey('premises.id', use_alter=True, name='fk_premises'))
    fuzzy = Column(Numeric(scale=1, precision=2))
    geonameId = Column(Integer)
    adminCode1 = Column(String(2))
    adminCode2 = Column(String(3))

    premises = relationship('Premises', foreign_keys=premises_id, backref='geonames_cached', post_update=True)


class Market(Base):

    __tablename__ = 'market'

    pk = Column(Integer, primary_key=True)
    name = Column(Text)
    address = Column(Text)
    po = Column(Text)
    city = Column(Text)
    state = Column(Text)
    zip = Column(String(5))
    zip_ext = Column(String(4))

    premises_id = Column(None, ForeignKey('premises.id'), nullable=True)
    premises = relationship('Premises')

    discriminator = Column('data_source', String(8))
    __mapper_args__ = {'polymorphic_on': discriminator}


class AMS(Market):

    __tablename__ = 'ams'
    __mapper_args__ = {'polymorphic_identity': __tablename__}

    pk = Column(None, ForeignKey('market.pk'), primary_key=True)
    id = Column(Integer)
    row = Column(Integer)


class APHIS(Market):

    __tablename__ = 'aphis'
    __mapper_args__ = {'polymorphic_identity': __tablename__}

    pk = Column(None, ForeignKey('market.pk'), primary_key=True)
    id = Column(String(5))
    row = Column(Integer)


class GIPSA(Market):

    __tablename__ = 'gipsa'
    __mapper_args__ = {'polymorphic_identity': __tablename__}

    pk = Column(None, ForeignKey('market.pk'), primary_key=True)
    row = Column(Integer)


class LMA(Market):

    __tablename__ = 'lma'
    __mapper_args__ = {'polymorphic_identity': __tablename__}

    pk = Column(None, ForeignKey('market.pk'), primary_key=True)
    id = Column(Integer)
    url = Column(Text)
    attributes = Column(Text)
    rank = Column(Text)
    country = Column(Text)
    description = Column(Text)
    email = Column(Text)
    fax = Column(Text)
    lat = Column(Text)
    lng = Column(Text)
    phone = Column(Text)
    featured = Column(Text)
    hours = Column(Text)
    tags = Column(Text)
    option_value = Column(Text)
    sl_pages_url = Column(Text)
    image = Column(Text)
    distance = Column(Float)
