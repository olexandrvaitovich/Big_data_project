from kafka import KafkaConsumer
import json
from pymongo import MongoClient

if __name__ == "__main__":
	client = MongoClient("localhost", 27017)
	db = client.mettups

	collections = [db.cities_col, db.city_for_country, db.events, db.citys, db.group_event]
	for inx, coll in enumerate(collections):
		consumer = KafkaConsumer(
		f'data_{inx+1}',
		bootstrap_servers=['{ip1}:9092,{ip2}:9092,{ip3}:9092'],
		auto_offset_reset='earliest',
		enable_auto_commit=True,
		value_deserializer=lambda x: json.loads(x.decode('utf-8')),
		consumer_timeout_ms=20000
		)
		if inx == 0:
			for i,val in enumerate(consumer):
				if i == 0:
					coll.insert_one({"_id":"1", "countries":[val.value["countrys"]]})
				else:
					coll.update_one({"_id":"1"},{"$push":{"countries": val.value["countrys"]}})
		elif inx == 1:
			for key in consumer:
				if coll.find({"country":key.value["country"]}).count() == 0:
					coll.insert_one({"country": key.value["country"], "citys": [key.value["citys"]]})
				else:
					for val in key.value["citys"]:
						coll.update_one({"country": key.value["country"]}, {"$addToSet": {"citys":val}})
		elif inx == 2:
			for event in consumer:
				coll.insert_one({"id": event.value["event_id"], "event": {key:event.value[key] for key in event.value.keys() if key != "event_id"}})
		elif inx == 3:
			for event in consumer:
				if coll.find({"city": event.value["city"]}).count() == 0:
					coll.insert_one({"city":event.value["city"],\
					                  "group_ids": event.value["group_ids"],\
					                  "group_names": event.value["group_names"]})
				else:
					for ids in event.value["group_ids"]:
						coll.update({"city": event.value["city"]},\
						      {"$addToSet": {"group_ids":ids}})
					for name in event.value["group_names"]:
						coll.update({"city": event.value["city"]},\
						      {"$addToSet": {"group_names":name}})
		elif inx == 4:
			for group in consumer:
				if coll.find({"group_id":group.value["group_id"]}).count() == 0:
					coll.insert_one({"group_id":group.value["group_id"], "events":group.value["event"]})
				else:
					coll.update_one({"group_id":group.value["group_id"]}, {"$push": {"events": group.value["event"]}})

		print("DONE")
	
