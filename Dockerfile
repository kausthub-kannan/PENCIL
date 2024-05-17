FROM python:3.10

# Set the working directory
WORKDIR /app
COPY ./etl .
COPY requirements.txt .
RUN pip install -r requirements.txt
CMD ["python", "pipeline.py"]
EXPOSE 3000