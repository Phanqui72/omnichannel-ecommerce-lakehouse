ARG DBT_BASE_IMAGE
FROM ${DBT_BASE_IMAGE}

ARG DBT_CORE_VERSION
ARG DBT_TRINO_VERSION

RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "dbt-core==${DBT_CORE_VERSION}" "dbt-trino==${DBT_TRINO_VERSION}"

WORKDIR /usr/app/dbt
ENTRYPOINT ["dbt"]
CMD ["--version"]