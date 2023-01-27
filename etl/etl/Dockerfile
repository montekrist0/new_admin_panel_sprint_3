FROM python:3.10

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
ENV HOME=/home/app
ENV APP_HOME=/home/app/etl

WORKDIR $APP_HOME

COPY ./requirements.txt .

RUN pip install --upgrade pip &&  \
    pip install --no-cache-dir -r requirements.txt

COPY . .

RUN groupadd -r app &&  \
    useradd -d $HOME -r -g app app && \
    chown app:app -R $HOME

RUN chmod +x  $APP_HOME/entrypoint.sh

USER app

ENTRYPOINT ["/home/app/etl/entrypoint.sh"]
