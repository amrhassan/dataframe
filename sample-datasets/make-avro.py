import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

row0 = {"name": "Virutal conference 1", "date": 25612345, "location":"New York", "speakers":["Speaker1","Speaker2"], "participants":["Participant1","Participant2","Participant3","Participant4","Participant5"], "seatingArrangement":{"Participant1":1, "Participant2":2, "Participant3":3, "Participant4":4, "Participant5":5}}

row1 = {"name": "Virutal conference 2", "date": 25612346, "location":"New Jersey", "speakers":["Speaker3","Speaker4"], "participants":["Participant11","Participant12","Participant13","Participant14","Participant15"], "seatingArrangement":{"Participant11":1, "Participant12":2, "Participant13":3, "Participant14":4, "Participant15":5}}

dataset = [row0, row1]

def make_avro():
    schema_json = """
        {"namespace": "demo.avro",
         "type": "record",
         "name": "Conference",
         "fields": [
             {"name": "name",       "type": "string"},
             {"name": "date",       "type": "long"},
             {"name": "location",   "type": "string"},
             {"name": "speakers",   "type": {"type":"array","items":"string"}},
             {"name": "participants", "type": {"type": "array", "items": "string"}},
             {"name": "seatingArrangement", "type": {"type": "map", "values": "int"}}
         ]
        }"""
    schema = avro.schema.Parse(schema_json)
    with open("participants.avro", "wb") as fp:
        writer = DataFileWriter(fp, DatumWriter(), schema, codec='deflate')
        for row in dataset:
            writer.append(row)
        writer.close()

make_avro()
