ARG python_base_image=python
ARG python_base_image_tag=3.9-slim
FROM ${python_base_image}:${python_base_image_tag} as builder

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc librdkafka-dev python-dev

COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels -r requirements.txt

FROM ${python_base_image}:${python_base_image_tag}

WORKDIR /app

COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/requirements.txt .

RUN pip install --no-cache /wheels/*
COPY kafka-mgm.py /app/
# We expected kafka clusters config to be in /app/clusters

CMD [ "python", "./kafka-mgm.py"]