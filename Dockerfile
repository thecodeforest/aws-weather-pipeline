
FROM public.ecr.aws/lambda/python:3.9

# copy from local to container root
COPY requirements.txt requirements.txt

# update pip
RUN pip install --upgrade pip

# executes any command and creates a new layer and commits the results
RUN pip install -r requirements.txt

# this is a special directory that is used to store the lambda function
# this is part of the base image
COPY app ${LAMBDA_TASK_ROOT}/app

# set the PYTHONPATH to the lambda function
# PYTHONPATH is an environment variable that
# is a list of directories to search for modules
ENV PYTHONPATH=${LAMBDA_TASK_ROOT}/app

# set the environment variables
ENV OUTPUT_BUCKET="raw-city-weather-data-1"

ENV GLUE_WORKFLOW_NAME="daily-weather-workflow"

# specify the entry point for the lambda function
CMD [ "lambda_handler.weather_collector"]