from flask import Flask, jsonify
from flask_restplus import Api, Resource

app = Flask(__name__)
api = Api(app = app)

name_space = api.namespace('batch', description='Batch API')

from pymongo import MongoClient
from datetime import datetime


client = MongoClient("localhost", 27017)

db = client.project_db2

country_data = db.country_data
country_groups_data = db.country_groups_data
state_topics_data = db.state_topics_data

PHRASE = "Value is not in the list :("


@name_space.route("/country_data/")
class Countries(Resource):
	def get(self):
		now = datetime.now().hour-1
		beg = now-6
		val = list(map(lambda x: {'country':x['country'], 'count':x['count']}, list(country_data.find({}))))
		if val is not None:
			return jsonify({'time_start':'{:02d}:00'.format(beg),
							'time_end':'{:02d}:00'.format(now),
							'statistics':val})
		else:
			return PHRASE


@name_space.route("/country_groups_data/")
class CountryGroups(Resource):
	def get(self):
		now = datetime.now().hour-1
		beg = now-3
		val = list(map(lambda x: {x['state']:x['collect_set(group_name)']}, list(country_groups_data.find({}))))
		if val is not None:
			return jsonify({'time_start':'{:02d}:00'.format(beg),
							'time_end':'{:02d}:00'.format(now),
							'statistics':val})
		else:
			return PHRASE


@name_space.route("/state_topics_data/")
class StateTopics(Resource):
	def get(self):
		now = datetime.now().hour-1
		beg = now-6
		val = list(map(lambda x: {x['country']:{x['topic']:x['count']}}, list(state_topics_data.find({}))))
		if val is not None:
			return jsonify({'time_start':'{:02d}:00'.format(beg),
							'time_end':'{:02d}:00'.format(now),
							'statistics':val})
		else:
			return PHRASE


if __name__ == '__main__':
	app.run()

