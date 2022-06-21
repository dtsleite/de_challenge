from flask import Flask,request, jsonify
import load_data

app = Flask(__name__)

@app.route('/')
def index():
    return jsonify({'message':'This service is running as expected!', 'status':'0'})


@app.route('/start_ingestion', methods=['GET'])
def start_ingestion():
    response = ''
    try:
        load_data.start_db()
        load_data.load_raw()
        response = jsonify({'message':'The process is running!', 'status':'0'})
    except Exception as ex:
        response = jsonify({'message':'There was an error to execute this method!', 'status':'1'})
    return response

@app.route('/avg_per_week', methods=['GET'])
def avg_per_week():
    response = ''
    try:
        rows = load_data.get_avg_per_week()
        response = rows
    except Exception as ex:
        response = jsonify({'message':'There was an error to execute this method!', 'status':'1'})
    return response    