# pylint: disable=too-few-public-methods
# Disabling too-few-public-methods pylint warning as the model classes will trigger that warning
# even if doesn't make sense in this context
import json

import requests
import sqlalchemy
from sqlalchemy import (Column, Float, ForeignKey, Integer, Sequence,
                        String)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

#Assigning declarative_base() to Base for future declaration of every table model
Base = declarative_base()

#Subclassing Base and defining every table model
#A note on code style: I used underscore for every variable before but in the models
#                      I use the same capitalization as the fields in the JSON
#                      for easier metaprogramming afterwards.
class Geo(Base):
    '''
    Model for geo table.
    '''
    __tablename__ = 'geo'
    id = Column(Integer, Sequence('geo_id_seq'), primary_key=True)
    lat = Column(Float)
    lng = Column(Float)
    address_id = Column(Integer, ForeignKey('address.id'))

class Address(Base):
    '''
    Model for address table.
    '''
    __tablename__ = 'address'
    id = Column(Integer, Sequence('address_id_seq'), primary_key=True)
    street = Column(String)
    suite = Column(String)
    city = Column(String)
    zipcode = Column(String)
    geo = relationship("Geo", backref="address", uselist=False)
    user_id = Column(Integer, ForeignKey('user.id'))

class Company(Base):
    '''
    Model for company table.
    '''
    __tablename__ = 'company'
    id = Column(Integer, Sequence('company_id_seq'), primary_key=True)
    name = Column(String)
    catchPhrase = Column(String)
    bs = Column(String)
    user_id = Column(Integer, ForeignKey('user.id'))

class User(Base):
    '''
    Model for user table.
    '''
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True)
    username = Column(String)
    email = Column(String)
    phone = Column(String)
    website = Column(String)

    address = relationship("Address", backref="user", uselist=False)
    company = relationship("Company", backref="user", uselist=False)

def get_users_data():
    '''
    Returns a list of dictionaries containing fake users data downloaded from
    https://jsonplaceholder.typicode.com/users.
    '''
    #Getting fake users data from https://jsonplaceholder.typicode.com/users using requests
    req = requests.get('https://jsonplaceholder.typicode.com/users')
    users_data = json.loads(req.text)
    print('Finished data extraction.')
    return users_data

def upper_first(string): #capitalize would change the case of the rest of the string
    '''
    Makes the first letter of a string upper case.
    '''
    return string[0].upper()+string[1:]

def add_user(json_dict, session):
    '''
    Add a user defined by a JSON object(Python dictionary) to the database
    using the connected session.
    By using metaprogramming and exploiting the naming conventions there is no need
    to specify different code for every table. Tables with
    "childs" such as Address are handled a bit more manually though.
    '''
    def get_address_instance(user_id, object_dict):
        '''
        Returns an instance of Address from a dictionary
        '''
        child = Address()
        geo = Geo()
        child.user_id = user_id
        for key, value in object_dict.items():
            if not isinstance(value, dict):
                setattr(child, key, value)
            elif key == 'geo':
                geo.address_id = child.id
                for key2, value2 in value.items():
                    setattr(geo, key2, value2)
        child.geo = geo
        return child

    sample_product = User()
    for key, value in json_dict.items():
        if isinstance(value, list):
            pass
        elif isinstance(value, dict):
            if key != 'address':
                class_name = upper_first(key)
                child = globals()[class_name]()
                child.user_id = json_dict['id']
                for key2, value2 in value.items():
                    setattr(child, key2, value2)
                setattr(sample_product, key, child)
            else:
                child = get_address_instance(json_dict['id'], value)
                setattr(sample_product, key, child)
        else:
            setattr(sample_product, key, value)
    session.add(sample_product)

def main():
    '''
    Extracts data from fake users json endpoint and converts the data into a relational database.
    '''
    users_data = get_users_data()

    print('Proceeding with relational database creation.')
    #Creating a fake_users.db SQLite database to store the data
    engine = sqlalchemy.create_engine('sqlite:///fake_users.db')

    #Creating the needed metadata after all the models have been defined
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    session.commit()

    for json_dict in users_data:
        add_user(json_dict, session)
    #Commit all the changes to the database
    session.commit()

    print('All done.')

if __name__ == '__main__':
    main()
