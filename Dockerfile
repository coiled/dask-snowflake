FROM mambaorg/micromamba:0.11.3

RUN mkdir /workspace


RUN micromamba install --yes \
    -c conda-forge \
    nomkl \
    git \
    python=3.8 \
    cython \
    coiled \
    c-compiler \
    cxx-compiler \
    dask \
    distributed \
    xgboost \
    dask-ml \
    xarray \
    pyarrow \
    tini \
    && \
    micromamba clean --all --yes


WORKDIR /workspace
RUN git clone https://github.com/snowflakedb/snowflake-connector-python.git
WORKDIR /workspace/snowflake-connector-python
RUN git checkout parallel-fetch-prpr

SHELL ["/bin/bash", "-c"]
COPY compile_snowflake.sh compile_snowflake.sh
RUN ./compile_snowflake.sh \
    && rm -rf build \
    && pip install dist/*.whl \
    && rm -rf dist \
    rm compile_snowflake.sh

RUN mkdir dask-snowflake

WORKDIR /workspace
COPY * dask_snowflake/
RUN pip install ./dask_snowflake


ENTRYPOINT ["tini", "-g", "--"]
