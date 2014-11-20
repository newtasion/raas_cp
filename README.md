QuickRelevance Framework 

Core engine code extracted from RaaSly. 
==============================

Setup Package Management
--------------

1. Make sure you have ```apt-get``` if you are running Ubuntu or ```brew``` if you are running Mac OS.
2. Run ```source bashrc``` in the root directory to have PYTHONPATH and DJANGO_SETTINGS_MODULE env variables properly setup.

```

How to start Django Server
--------------

Under repo root, run

```bash
cd server
export PYTHONPATH=$(pwd)/..:$PYTHONPATH
python manage.py runserver 0.0.0.0:8000
# now you can browse the site at http://localhost:8000
```

How to start celery
-------------

Celery is a background worker management process

To start celery, you need rabbitmq server running

```bash
rabbitmq-server
```

Then Start the celery workers

```bash
tools/start_celery.sh
```