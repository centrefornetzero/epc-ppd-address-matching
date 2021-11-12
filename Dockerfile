FROM python:3.9.6-slim-buster AS dependencies

RUN apt-get update && apt-get -y upgrade

RUN pip install pipenv
ENV PIPENV_VENV_IN_PROJECT=1

RUN useradd --create-home user && chown -R user /home/user
USER user
WORKDIR /home/user/src

COPY Pipfile* .
RUN pipenv sync --keep-outdated
ENV PATH="/home/user/src/.venv/bin:$PATH"
ENV PYTHONPATH=.


FROM dependencies AS runtime

COPY  . .
ENTRYPOINT ["python", "-m", "app"]


FROM dependencies AS testrunner

RUN pipenv sync --keep-outdated --dev
ENV PYTEST_ADDOPTS="-p no:cacheprovider"
COPY . .
CMD ["pytest"]
