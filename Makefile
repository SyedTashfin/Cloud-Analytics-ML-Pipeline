SHELL := /bin/bash
PYTHON ?= python3
VENV = .venv
PIP := $(VENV)/bin/pip
PYTHON_VENV := $(VENV)/bin/python
CONFIG ?= config.yaml
PYTHONPATH ?= src
FORCE ?= 0
FORCE_FLAG := $(if $(filter 1 true TRUE yes YES,$(FORCE)),--force,)
SMOKE_LIMIT ?= 2000000
SMOKE_PROCESSED ?= data/processed_smoke
SMOKE_SUFFIX ?= _smoke

export PYTHONPATH

.PHONY: venv deps setup check_venv env_check clean_interim clean_processed \
	download_data verify_raw to_parquet build_features train evaluate plots \
	dashboard_data dashboard rerun_from_parquet rerun_all_force smoke \
	gcp_auth_check gcp_setup gcp_upload_raw gcp_package gcp_to_parquet \
	gcp_build_features gcp_train gcp_evaluate gcp_plots gcp_run_all

venv:
	@test -d $(VENV) || $(PYTHON) -m venv $(VENV)

deps: venv
	$(PIP) install -r requirements.txt

setup: venv deps

check_venv:
	@test -x $(PYTHON_VENV) || ( \
		echo "Virtual environment not found at $(VENV). Run 'make setup' first."; \
		exit 1; \
	)

env_check: check_venv
	$(PYTHON_VENV) src/utils/env_check.py

clean_interim:
	rm -rf data/interim

clean_processed:
	rm -rf data/processed data/processed_smoke

download_data: check_venv
	$(PYTHON_VENV) -m clickstream.pipelines.download_data --config $(CONFIG)

verify_raw: env_check
	$(PYTHON_VENV) src/utils/verify_raw.py --config $(CONFIG)

to_parquet: verify_raw
	$(PYTHON_VENV) -m clickstream.pipelines.to_parquet --config $(CONFIG) \
		$(FORCE_FLAG)

build_features: check_venv
	$(PYTHON_VENV) -m clickstream.pipelines.build_features --config $(CONFIG) \
		$(FORCE_FLAG)

train: check_venv
	$(PYTHON_VENV) -m clickstream.pipelines.train --config $(CONFIG)

evaluate: check_venv
	$(PYTHON_VENV) -m clickstream.pipelines.evaluate --config $(CONFIG)

plots: check_venv
	$(PYTHON_VENV) -m clickstream.pipelines.plots --config $(CONFIG)

dashboard_data: check_venv
	$(PYTHON_VENV) -m clickstream.pipelines.export_dashboard_data --config $(CONFIG) \
		$(FORCE_FLAG)

dashboard: check_venv
	$(PYTHON_VENV) -m streamlit run dashboard/app.py

rerun_from_parquet:
	$(MAKE) build_features
	$(MAKE) train
	$(MAKE) evaluate
	$(MAKE) plots

rerun_all_force:
	$(MAKE) to_parquet FORCE=1
	$(MAKE) build_features FORCE=1
	$(MAKE) train
	$(MAKE) evaluate
	$(MAKE) plots

smoke: verify_raw
	$(PYTHON_VENV) -m clickstream.pipelines.to_parquet --config $(CONFIG) \
		--force --limit $(SMOKE_LIMIT) --processed-root $(SMOKE_PROCESSED) \
		--reports-suffix $(SMOKE_SUFFIX)
	$(PYTHON_VENV) -m clickstream.pipelines.build_features --config $(CONFIG) \
		--force --processed-root $(SMOKE_PROCESSED) \
		--reports-suffix $(SMOKE_SUFFIX)

gcp_auth_check:
	./scripts/gcp_auth_check.sh

gcp_setup:
	./scripts/gcp_setup.sh

gcp_upload_raw:
	./scripts/gcp_upload_raw.sh

gcp_package:
	./scripts/gcp_package.sh

gcp_to_parquet:
	./scripts/gcp_submit.sh clickstream-to-parquet-$$(date +%Y%m%d-%H%M%S) \
		src/clickstream/pipelines/to_parquet.py

gcp_build_features:
	./scripts/gcp_submit.sh clickstream-build-features-$$(date +%Y%m%d-%H%M%S) \
		src/clickstream/pipelines/build_features.py

gcp_train:
	./scripts/gcp_submit.sh clickstream-train-$$(date +%Y%m%d-%H%M%S) \
		src/clickstream/pipelines/train.py

gcp_evaluate:
	./scripts/gcp_submit.sh clickstream-evaluate-$$(date +%Y%m%d-%H%M%S) \
		src/clickstream/pipelines/evaluate.py

gcp_plots:
	./scripts/gcp_submit.sh clickstream-plots-$$(date +%Y%m%d-%H%M%S) \
		src/clickstream/pipelines/plots.py

gcp_run_all:
	./scripts/gcp_run_all.sh
