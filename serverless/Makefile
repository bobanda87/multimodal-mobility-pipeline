# SAM custom build: copies shared utils + the collector package, installs light dependencies.
# pandas / pyarrow / numpy / boto3 come from the AWS SDK for pandas layer, so they are not installed here.

# SAM runs this Makefile in a temp copy of serverless/, so locate the project root from ARTIFACTS_DIR
# (<root>/serverless/.aws-sam/build/<Function>); this assumes the default build dir.
ROOT = $(abspath $(ARTIFACTS_DIR)/../../../..)

define package
	mkdir -p $(ARTIFACTS_DIR)
	cp -r $(ROOT)/utils $(ARTIFACTS_DIR)/utils
	cp -r $(ROOT)/$(1) $(ARTIFACTS_DIR)/$(1)
	pip install -r requirements-lambda.txt -t $(ARTIFACTS_DIR) --upgrade \
		--platform manylinux2014_x86_64 --only-binary=:all: --python-version 3.12 --implementation cp
	find $(ARTIFACTS_DIR) -name __pycache__ -type d -prune -exec rm -rf {} +
endef

build-AvinorFunction:
	$(call package,avinor)

build-EnturFunction:
	$(call package,entur)

build-EnturNationalFunction:
	$(call package,entur)

build-EnturSiriNationalFunction:
	$(call package,entur)

build-OsloBysykkelFunction:
	$(call package,oslobysykkel)

build-VegvesenFunction:
	$(call package,vegvesen)
