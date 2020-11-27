#!/usr/bin/env bash

dot data_model.dot -Tpng -o data_model.png
dot spark_plan.dot -Tpng -o spark_plan.png