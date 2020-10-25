# Flask app config
# Logging config
from logging.config import dictConfig

from flask import Flask

from serving.microservice.config import ApplicationConfig

dictConfig({
    'version': 1,
    'formatters': {'rascacielos-formatter': {
        # DATE <#> LEVEL <#> MODULE <#> UID <#> CLASS <#> MESSAGE
        'format': '%(asctime)s.%(msecs)03d|%(levelname)s|%(filename)s|Process-%(process)d %(threadName)s|%(funcName)s|%(message)s',
        'datefmt': '%Y%m%d%H%M%S'
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://flask.logging.wsgi_errors_stream',
        'formatter': 'rascacielos-formatter'
    }},
    'root': {
        'level': 'DEBUG',
        'handlers': ['wsgi']
    }
})

def create_application(config_class=ApplicationConfig):
    # Flask app config
    app = Flask(__name__)
    app.config.from_object(config_class)
    return app

application = create_application()