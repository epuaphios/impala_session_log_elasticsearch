import urllib.request as urllib2
from bs4 import BeautifulSoup
import json
from elasticsearch import Elasticsearch

def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 1500}])
    if _es.ping():
        print('Yay Connected')
    else:
        print('Awww it could not connect!')
    return _es



def delete_index(es_object, index_name):
    deleted = False
    try:
        es.indices.delete(index=index_name, ignore=[400, 404])
        print('Delete Index')
        deleted = True
    except Exception as ex:
        print(str(ex))
    finally:
        return deleted      

def store_record(elastic_object, index_name, record):
    is_stored = True
    try:
        outcome = elastic_object.index(index=index_name, body=record)
        print(outcome)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored

if __name__ == '__main__':
    url = "impala.coordinator.url:25000/sessions"
    url2 = "impala.coordinator.url:25000/sessions"
    es = connect_elasticsearch()
    if delete_index(es, 'test'):
        soup = BeautifulSoup(urllib2.urlopen(url).read(), features="html.parser")
        for table in soup.find_all('table'):
            keys = [th.get_text(strip=True) for th in table.find_all('th')]
            for row in table.findAll('tr'):
               values = [td.get_text(strip=True) for td in row.find_all('td')]
               d = dict(zip(keys, values))
               if es is not None:
                    out = store_record(es, 'test', d)
            print('Data url indexed successfully')
        soup = BeautifulSoup(urllib2.urlopen(url2).read(), features="html.parser")
        for table in soup.find_all('table'):
            keys = [th.get_text(strip=True) for th in table.find_all('th')]
            for row in table.findAll('tr'):
               values = [td.get_text(strip=True) for td in row.find_all('td')]
               d = dict(zip(keys, values))
               if es is not None:
                    out = store_record(es, 'test', d)
            print('Data url2 indexed successfully')
