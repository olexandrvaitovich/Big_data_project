from pyspark.sql.types import StructType, StringType, FloatType, IntegerType, ArrayType, LongType


data_schema = StructType().\
                    add('venue', StructType().\
                            add('venue_name', StringType()).\
                            add('lon', FloatType()).\
                            add('lat', FloatType()).\
                            add('venue_id', LongType())).\
                    add('visibility', StringType()).\
                    add('response', StringType()).\
                    add('guests', IntegerType()).\
                    add('member', StructType().\
                            add('member_id', IntegerType()).\
                            add('photo', StringType()).\
                            add('member_name', StringType())).\
                    add('rsvp_id', IntegerType()).\
                    add('mtime', LongType()).\
                    add('event', StructType().\
                            add('event_name', StringType()).\
                            add('event_id', StringType()).\
                            add('time', LongType()).\
                            add('event_url', StringType())).\
                    add('group', StructType().\
                            add('group_topics', ArrayType(StructType().\
                                    add('urlkey', StringType()).\
                                    add('topic_name', StringType()))).\
                            add('group_city', StringType()).\
                            add('group_country', StringType()).\
                            add('group_id', IntegerType()).\
                            add('group_name', StringType()).\
                            add('group_lon', FloatType()).\
                            add('group_urlname', StringType()).\
                            add('group_lat', FloatType()).\
                            add('group_state', StringType()))

