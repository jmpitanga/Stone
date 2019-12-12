FROM python:3

WORKDIR ./Stone

COPY req.txt

RUN pip install -r req.txt

COPY case.py

CMD python case.py