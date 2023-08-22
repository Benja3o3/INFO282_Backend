from flask import Flask
from psycopg2 import connect

app = Flask(__name__)

host = "localhost"
port = 8080
dbname = ""


@app.get("/")
def hello():
    return "Hello World!"


if __name__ == "__main__":
    app.run(debug=True)
