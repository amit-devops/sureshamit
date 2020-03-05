from app import celery, create_app  # noqa

# Celery Workers import celery instance from this file
# celery instance is initializes inside of the create_app
app = create_app()
app.app_context().push()
