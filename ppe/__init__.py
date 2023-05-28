
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from google.cloud import secretmanager # Import the Secret Manager client library.
import google_crc32c
import os
#import sqlalchemy as sqlalchemy #to save to db

#link
# https://github.com/CoreyMSchafer/code_snippets/tree/master/Python/Flask_Blog

def get_secret(project_id, secret_id, version_id):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Verify payload checksum.
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)
    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        print("Data corruption detected.")
        return response
    else:
        payload = response.payload.data.decode("UTF-8")
        return payload

app = Flask(__name__)

db_user = "root"
db_name = "raw_data"
db_password = get_secret("mister-market-project", "db_password", "1")
db_hostname = get_secret("mister-market-project", "db_hostname", "1")
port = "3306"

#this doesn't work because I'm trying to run from windows which doesnt support unix_socket connections
#cloud_sql_instance = "mister-market-project:us-central1:mister-market-db"
#app.config["SQLALCHEMY_DATABASE_URI"] = f"mysql+pymysql://{db_user}:{db_pass}@/{db_name}?unix_socket=/cloudsql/{cloud_sql_instance}"

app.config["SQLALCHEMY_DATABASE_URI"] = f"mysql+pymysql://{db_user}:{db_password}@{db_hostname}:{port}/{db_name}"


db = SQLAlchemy(app)

from ppe import routes
