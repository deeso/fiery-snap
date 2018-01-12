FROM python:3.5
ADD config.toml config.toml
ADD python_cmd.sh python_cmd.sh
ADD main.py main.py
ADD package package
RUN cd package; pip install -r requirements.txt .; cd ..; rm -r package


CMD [ "sh", "python_cmd.sh" ]

EXPOSE 10006/tcp
