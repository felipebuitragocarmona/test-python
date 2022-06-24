from flask import Flask
from flask import jsonify
from flask import request
from flask_cors import CORS
from RefactoredCode.IndividualNotifications import IndividualNotifications
app=Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route("/notifications",methods=['POST'])
def notifications():
    data = request.get_json()
    new_process=IndividualNotifications(data["message"])
    new_process.startProcessForProfiles(data["profiles_to"])
    response={
     "message":"Notification process completed"
    }
    return jsonify(response)

if __name__=='__main__':
    print("Server running ...")
    from waitress import serve
    serve(app, host="127.0.0.1", port=8080)

