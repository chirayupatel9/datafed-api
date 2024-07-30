from flask import Flask, request, jsonify
# import datafed.CommandLib
from datafed.CommandLib import API


import json

app = Flask(__name__)
df_api = API()


@app.route('/data', methods=['POST'])
def create_record():
    data = request.json
    title = data.get('title')
    metadata = data.get('metadata', {})
    context = data.get('context', None)  # Optional context parameter

    try:
        if context:
            df_api.setContext(context)
        response = df_api.dataCreate(title, metadata=json.dumps(metadata))
        return jsonify(response.to_dict()), 201
    except Exception as e:
        return str(e), 400


@app.route('/data/<record_id>', methods=['GET'])
def read_record(record_id):
    try:
        response = df_api.dataGet(record_id)
        return jsonify(response.to_dict()), 200
    except Exception as e:
        return str(e), 404


@app.route('/data/<record_id>', methods=['PUT'])
def update_record(record_id):
    data = request.json
    metadata = data.get('metadata', {})
    try:
        response = df_api.dataUpdate(record_id, metadata=json.dumps(metadata))
        return jsonify(response.to_dict()), 200
    except Exception as e:
        return str(e), 400


@app.route('/data/<record_id>', methods=['DELETE'])
def delete_record(record_id):
    try:
        df_api.dataDelete(record_id)
        return '', 204
    except Exception as e:
        return str(e), 400


@app.route('/transfer', methods=['POST'])
def transfer_data():
    data = request.json
    source_id = data.get('source_id')
    dest_collection = data.get('dest_collection')
    try:
        # Retrieve source data record details
        source_record = df_api.dataGet(source_id)
        source_details = source_record.to_dict()

        # Create a new record in the destination collection
        new_record = df_api.dataCreate(
            title=source_details['title'],
            description=source_details.get('description', ''),
            metadata=json.dumps(source_details.get('metadata', {})),
            parent=dest_collection
        )
        new_record_id = new_record.id

        # Transfer the actual data file using Globus endpoints
        transfer_response = df_api.dataMove(
            source_id,
            new_record_id
        )

        return jsonify({
            "new_record_id": new_record_id,
            "transfer_status": transfer_response.to_dict()
        }), 200
    except Exception as e:
        return str(e), 400


if __name__ == '__main__':
    app.run(debug=True)
