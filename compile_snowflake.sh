#!/bin/bash

export SF_ARROW_LIBDIR=${CONDA_PREFIX}/lib
export SF_NO_COPY_ARROW_LIB=1
printenv

python setup.py build_ext
python setup.py bdist_wheel


