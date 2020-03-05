import os
from flask import jsonify
from app import create_app

application = create_app()


@application.route("/urls")
def urls():
    return jsonify(
        {
            "APP_MODE": str(os.getenv("APP_MODE")),
            "URLs registered": f"{str(application.url_map)}",
        }
    )


if __name__ == "__main__":
    application.run()
