from flask import Flask
from flask_restplus import Api, Resource

app = Flask(__name__)
api = Api(app = app)

name_space = api.namespace('streaming', description='Streaming API')


from pymongo import MongoClient


client = MongoClient("localhost", 27017)
db = client.mettups

cities_col = db.cities_col
city_for_country = db.city_for_country
events = db.events
citys = db.citys
group_event = db.group_event

PHRASE = "Value is not in the list :("


@name_space.route('/all_countries/')
class Countries(Resource):
	def get(self):
		val = cities_col.find_one({})
		if val is not None:
			return str(val["countries"])
		else:
			return PHRASE


@name_space.route("/cities_for_country/<country>")
class CitiesForCountry(Resource):
	def get(self):
		val = city_for_country.find_one({"country":country})
		if val is not None:
			return str(val["citys"])
		else:
			return PHRASE


@name_space.route("/event_id/<id_>")
class Event(Resource):
	def get_event(self, id_):
		val = events.find_one({"id": id_})
		if val is not None:
			return str(val["event"])
		else:
			return PHRASE


@name_space.route("/city_group/<city>")
class CityGroup(Resource):
	def get(self, city):
		val = citys.find_one({"city":city})
		if val is not None:
			return str({key:val[key] for key in val.keys() if key != "_id"})
		else:
			return PHRASE


@name_space.route("/group_event/<id_>")
class EventGroup(Resource):
	def get(self, id_):
		val = group_event.find_one({"group_id":int(id_)})
		if val is not None:
			return str(val["events"])
		else:
			return PHRASE


if __name__ == '__main__':
	app.run()