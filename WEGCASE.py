import json
import configparser
import time
import logging
from dataclasses import dataclass
from sqlalchemy import create_engine
from sqlalchemy import Column, String, Numeric, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship

Base = declarative_base()   

@dataclass
class Category(Base):
    __tablename__ = 'category'
    category_id:int = Column(Numeric,primary_key=True)
    category_name: str = Column(String)

    hotels = relationship("Hotel", back_populates="category")
    __table_args__ = {'schema': 'hotel_schema'}

@dataclass
class Chain(Base):
    __tablename__ = 'chain'
    chain_id: int = Column(Numeric,primary_key=True)
    chain_name: str = Column(String)

    hotels = relationship("Hotel", back_populates="chain")
    __table_args__ = {'schema': 'hotel_schema'}

@dataclass
class Hotel(Base):
    __tablename__ = 'hotel'
    hotel_id: int = Column(Numeric,primary_key=True)
    hotel_name: str = Column(String)
    category_id: int = Column(Numeric,ForeignKey('hotel_schema.category.category_id'))
    chain_id: int = Column(Numeric,ForeignKey('hotel_schema.chain.chain_id'))
    location: str = Column(String)

    category = relationship("Category", back_populates="hotels")
    chain = relationship("Chain", back_populates="hotels")
    __table_args__ = {'schema': 'hotel_schema'}

class JsonReader:
    def __init__(self,path) -> None:
        self.data = self.reader_json(path)

    @staticmethod
    def reader_json(path):
        try:
            file = open(path, 'r')
            data = json.load(file).values()
            file.close()
                
        except Exception as e:
            error = f"JSON data could not be read due to {e}"
            logging.error(error)
            raise Exception(error)

        if len(data) == 0:
            error = "JSON data is empty"
            logging.error(error)
            raise Exception(error)
            
        return data

class ObjectCreater:

    @staticmethod
    def category_maker(row) -> Category:
        category_data = row.get("category")

        try:
            id = int(category_data.get("id"))     #eger int olamaycak bir degere sahipse veya category_data bos ise hata atar.
        except Exception as e:
            warn = Warning(f"Category is not created due to {e}")
            logging.warning(warn)
            return None
        
        if id >= 0:
            name = category_data.get("name") 
            return Category(category_id=id,category_name=name)
        
        warn = Warning("Category is not created due to id smaller than 0")
        logging.warning(warn)
        return None

    @staticmethod
    def chain_maker(row) -> Chain:
        chain_data = row.get("chain")

        try:
            id = int(chain_data.get("id"))

        except Exception as e:
            warn = Warning(f"Category is not created due to {e}")
            logging.warning(warn)
            return None
        
        if id >= 0:
            name = chain_data.get("name")
            return Chain(chain_id=id,chain_name=name)

        warn = Warning("Chain is not created due to id smaller than 0")
        logging.warning(warn)
        return None
     
    def hotel_maker(self,row,chain:Chain,category:Category) -> Hotel:
        location_data = self.location_check(row)

        try:
            id = int(row.get("property_id"))
        except Exception as e:
            error = f"Hotel is not created due to {e}"
            logging.error(error)
            return None
        name = row.get("name")

        chain_id = None    
        category_id = None

        if chain != None:
            chain_id = chain.chain_id

        if category != None:
            category_id = category.category_id

        if name != None and id >= 0:
            return Hotel(hotel_id=id,hotel_name=name,category_id=category_id,chain_id=chain_id,location=location_data)

        warn = Warning("Hotel is not created due to id smaller than 0 or name is invalid")
        logging.warning(warn)
        return None

    @staticmethod
    def location_check(row):
        location_data = row.get("location")

        if location_data != None:
            try:
                obf_req = location_data.get("obfuscation_required")
                obf_coor = location_data.get("obfuscated_coordinates")
            except Exception as e:
                warn = f"Failed to parse obfuscated_coordinates data due to {e}"
                logging.warning(warn)
                obf_req = False
            
            if not obf_req:
                coor = location_data.get("coordinates")
                try:
                    lat = coor.get("latitude")
                    long = coor.get("longitude")
                    lat_long = f"{lat},{long}"
                    return lat_long
                except Exception as e:
                    warn = f"Failed to parse coordinates data due to {e}"
                    logging.warning(warn)
                    return None

            else:
                try:
                    obf_lat = obf_coor.get("latitude")
                    obf_long = obf_coor.get("longitude")
                    lat_long = f"{obf_lat},{obf_long}"
                    return lat_long
                except Exception as e:
                    warn = f"Failed to parse obfuscated_coordinates data due to {e}"
                    logging.error(warn)
                    return None
                    
        else:
            return None

class DbProcesser:
    def __init__(self,config):
        self.session = self.create_session(config)

    @staticmethod
    def create_session(config):
        try:
            engine = create_engine(f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}")
            Session = sessionmaker(bind=engine)
            session = Session()
        except Exception as e:
            error = f"Session could not be established due to {e}"
            logging.error(error)
            raise Exception(error)
        return session
    
    def insert_data(self,data):
        try:  
            self.session.add_all(data)
            self.session.commit()
        except TypeError as e:
            error = f"Failed to insert data into database due to {e}"
            logging.error(error)
            raise Exception(error)
        
class DataPipeline:
    def __init__(self,object_creater:ObjectCreater,db_processer:DbProcesser,data):
        self.object_creater = object_creater
        self.db_processer = db_processer
        self.data = data

    def run(self):

        adder_list = []

        try:
            for row in self.data:
                category = self.object_creater.category_maker(row)
                chain = self.object_creater.chain_maker(row)
                hotel = self.object_creater.hotel_maker(row,chain,category)


                if category != None and category.category_name != None and category not in adder_list:
                    adder_list.append(category)

                if chain != None and chain.chain_name != None and chain not in adder_list:
                    adder_list.append(chain)

                if hotel != None and hotel not in adder_list:
                    adder_list.append(hotel)
                    
        except Exception as e:
            error = f"Datapipeline has failed due to {e}"
            logging.error(error)
            raise Exception(error)

        self.db_processer.insert_data(adder_list)
    
if __name__ == "__main__":

    start = time.time()
    config = configparser.ConfigParser()
    config.read('config.ini')
    logging.basicConfig(filename=config['LOG']['log_file'], level=config['LOG']['log_level'])

    objcet_creater = ObjectCreater()
    db_processer = DbProcesser(config['DB'])  
    json_reader = JsonReader(config['DATA']['path'])  #config['DATA']['path']

    data_migrater = DataPipeline(objcet_creater,db_processer,json_reader.data)
    data_migrater.run()
    db_processer.session.close()

    end = time.time()
    print(end - start)

## to do dbde olan verilerin kontrolu