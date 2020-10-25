from serving.microservice.routes import application
import os

if __name__ == '__main__':
    application.run(host='127.0.0.1', port=8081)