from src.app import app

HOST = "localhost"
PORT = 8080
DEBUG = True


if __name__ == "__main__":
    app.run(host=HOST, debug=DEBUG, port=PORT)
