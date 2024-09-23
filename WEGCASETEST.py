import unittest
from unittest.mock import patch, MagicMock
from WEGCASE import JsonReader, ObjectCreater, DbProcesser, DataPipeline , Category,Chain,Hotel



class TestObjectCreater(unittest.TestCase):

        def test_category_maker_valid(self):
            row = {"category": {"id": 1, "name": "Hotel"}}
            category = ObjectCreater.category_maker(row)
            self.assertIsNotNone(category)
            self.assertEqual(category.category_id, 1)
            self.assertEqual(category.category_name, "Hotel")
        
        def test_category_maker_invalid_id(self):
            row = {"category": {"id": "invalid", "name": "Hotel"}}
            category = ObjectCreater.category_maker(row)
            self.assertIsNone(category)


        def test_chain_maker_valid(self):
            row = {"chain": {"id": 0, "name": "Independent"}}
            chain = ObjectCreater.chain_maker(row)
            self.assertIsNotNone(chain)
            self.assertEqual(chain.chain_id, 0)
            self.assertEqual(chain.chain_name, "Independent")

        def test_chain_maker_invalid_id(self):
            row = {"chain": {"id": "invalid", "name": "Independent"}}
            chain = ObjectCreater.chain_maker(row)
            self.assertIsNone(chain)


        def test_hotel_maker_valid(self):
            row = {"property_id": 10000032, "name": "hotel 107", "location": {"coordinates": {"latitude": 11.111, "longitude": 22.222}}}
            category = Category(category_id=1, category_name="Hotel")
            chain = Chain(chain_id=1, chain_name="Independent")
            object_creater = ObjectCreater()
            hotel = ObjectCreater.hotel_maker(self=object_creater,row=row, chain=chain, category=category)
            self.assertIsNotNone(hotel)
            self.assertEqual(hotel.hotel_id, 10000032)
            self.assertEqual(hotel.hotel_name, "hotel 107")
            self.assertEqual(hotel.location, "11.111,22.222")

        def test_hotel_maker_invalid_id(self):
            row = {"property_id": "invalid_id", "name": "hotel 107"}
            category = Category(category_id=1, category_name="Hotel")
            chain = Chain(chain_id=1, chain_name="Independent")
            hotel = ObjectCreater().hotel_maker(row, chain, category)
            self.assertIsNone(hotel)

        def test_hotel_maker_invalid_name(self):
            row = {"property_id": 10000032, "name": None}
            category = Category(category_id=1, category_name="Hotel")
            chain = Chain(chain_id=1, chain_name="Independent")
            hotel = ObjectCreater().hotel_maker(row, chain, category)
            self.assertIsNone(hotel)

        def test_location_check_valid(self):
            row = {"location":{"coordinates":{"latitude":42.60803,"longitude":8.864105},"obfuscation_required":False}}
            location_data = ObjectCreater.location_check(row)
            self.assertIsNotNone(location_data)

        def test_location_check_empty(self):
            row = {}
            location_data = ObjectCreater.location_check(row)
            self.assertIsNone(location_data)

        def test_location_check_invalid_obf(self):
            row = {"location":{"coordinates":{"latitude":42.60803,"longitude":8.864105},"obfuscation_required":True}}
            location_data = ObjectCreater.location_check(row)
            self.assertIsNone(location_data)

        def test_location_check_invalid_coor(self):
            row = {"location":{"coordinatesT":{"latitude":42.60803,"longitude":8.864105},"obfuscation_required":False}}
            location_data = ObjectCreater.location_check(row)
            self.assertIsNone(location_data)

class TestDbProcesser(unittest.TestCase):


    def test_create_session_valid(self):
        db_processer = DbProcesser({'user': 'admin','password': 'admin', 'host': 'localhost', 'port': '5432', 'dbname': 'hotel_providers'})
        self.assertIsNotNone(db_processer.session)

    def test_create_session_invalid(self):
        config = {'user': 'user','password': 'pass', 'host': 'localhost', 'port': '5432', 'dbname': 'test_db'}
        dbp = DbProcesser(config=config)
        self.assertRaises(Exception,dbp.create_session(config=config))

    @patch('sqlalchemy.orm.session.Session.add_all')
    @patch('sqlalchemy.orm.session.Session.commit')
    def test_insert_data_valid(self,mock_commit,mock_add_all):
        config = {'user': 'admin','password': 'admin', 'host': 'localhost', 'port': '5432', 'dbname': 'hotel_providers'}
        db_processor = DbProcesser(config)
        data = [Category(category_id=1,category_name='cat_name'),Chain(chain_id=2,chain_name='cha_name'),Hotel(hotel_id=12321,hotel_name='hot_name',category_id=21321,chain_id=3123,location='11.111,22.222')]
        self.mock_session = MagicMock()
        db_processor.insert_data(data)
        mock_add_all.assert_called_once_with(data)
        mock_commit.assert_called_once()

class TestJsonReader(unittest.TestCase):
    def test_reader_json_valid(self):
        path = 'test1.json'
        data =[{"property_id":100005227}]
        json_reader = JsonReader(path=path)
        test_data = json_reader.reader_json(path=path)
        self.assertEqual(data, list(test_data))
    
    def test_reader_json_empty(self):
        path = 'test2.json'
        with self.assertRaises(Exception):
            JsonReader(path)
    
    def test_reader_json_path(self):
        path = 'dummy.json'
        with self.assertRaises(Exception):
            JsonReader(path)
        
class TestDataPipeline(unittest.TestCase):

    def setUp(self):
        self.mock_object_creater = MagicMock(ObjectCreater)
        self.mock_db_processer = MagicMock(DbProcesser)
    
        self.mock_data = [{"property_id":10000527,"category":{"id":"1","name":"Hotel"},"chain":{"id":"0","name":"Independent"},"location":{"coordinates":{"latitude":42.60803,"longitude":8.864105},"obfuscation_required":'false'},"name":"hotel 0"}]
        self.data_pipeline = DataPipeline(object_creater=self.mock_object_creater,db_processer=self.mock_db_processer,data=self.mock_data)

    def test_run_success(self):

        self.mock_object_creater.category_maker.return_value = Category(category_id=1, category_name="Hotel")
        self.mock_object_creater.chain_maker.return_value = Chain(chain_id=1, chain_name="Independent")
        self.mock_object_creater.hotel_maker.return_value = Hotel(hotel_id=1, hotel_name="Hotel1", category_id=1, chain_id=1, location="40.7128,-74.0060")
    
        self.data_pipeline.run()

        self.mock_db_processer.insert_data.assert_called_once()
        adder_list = self.mock_db_processer.insert_data.call_args[0][0]

        self.assertEqual(len(adder_list), 3)
        self.assertIsInstance(adder_list[0], Category)
        self.assertIsInstance(adder_list[1], Chain)
        self.assertIsInstance(adder_list[2], Hotel)

    def test_run_exception(self):

        self.mock_object_creater.category_maker.side_effect = Exception("Mocked exception")
        with self.assertRaises(Exception):
            self.data_pipeline.run()


if __name__ == '__main__':
    unittest.main()