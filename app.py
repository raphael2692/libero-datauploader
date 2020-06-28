from datauploader import DataUploader
from flask import Flask
import requests
import json
app = Flask(__name__)


@app.route('/')
def main():
    desc = "<h1>DB client v 0.1</h1>"
    return desc


@app.route('/updatedb')
def updatedb():
    db = DataUploader(db_url="<uri del vostro db>",resource_url="<url della vostra app Heroku>/liberoquotidiano")
    return db.upload_data()
    
if __name__ == '__main__':
    # Threaded option to enable multiple instances for multiple user access support
    app.run(threaded=True, port=5000)