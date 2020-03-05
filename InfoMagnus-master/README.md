
## Development Setup
##### 1. Install system requirements
###### Windows:
- Docker Desktop: https://www.docker.com/products/docker-desktop
- Python 3.7: https://www.python.org/downloads/
- Git: https://git-scm.com/
###### Ubuntu:
- Docker https://snapcraft.io/install/docker/ubuntu
- Python 3.7 https://linuxize.com/post/how-to-install-python-3-7-on-ubuntu-18-04/

##### 2. Clone Repository
- https://github.com/DSDPartnersInc/InfoMagnus

##### 3. Install Python development packages
```
# Step 1. Change into top-level directory of repository

# Step 2. Create virtual environment
## Ubuntu
pip3 install virtualenv
virtualenv -p python3.7 DSDPenv
source ./DSDPenv/bin/activate

## Windows
python3 -m venv DSDPenv
source ./DSDPenv/bin/activate

# Step 3. Install Python dev requirements
cd ./dsdp
pip install -r requirements-dev.txt

#Step 4.  Run from top-level directory of repository to
# install pre-commit hooks
cd ..
pre-commit install

# Test that the hooks were installed correctly
# This command can be used at anytime to run the hooks against all files
pre-commit run --all-files
```

##### 3. Run application using Docker
```
# Run command in top-level directory of repository
docker-compose up --build 
```

##### 4. Verify the app is running
-  Check that the flask app is responding and the celery worker is alive
    - Controller Container: http://localhost:5000/api/health/check/
    - Denormalizer Container: http://localhost:5001/api/health/check/
    - Prediction Container: http://localhost:5002/api/health/check/
-   Expected response
```
{
  "error": "",
  "status": "UP"
}
```

Contributing Guidelines 
---
The following checks are enfored by Github Actions and should successfully pass prior to pushing any commits
*Note: All check are done in `./dsdp` folder.*
1) Code formatting check `black . `
2) Linting check `flake8 . `
3) Type annotation check `mypy . `

