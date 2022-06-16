PYTHON=$(shell which python)
SSH_KEYS=$(shell echo $(HOME)/.ssh/id_rsa)

#################################################################################
# Clean	                                                                        #
#################################################################################
clean:
	rm -rf .pytest_cache
	rm -rf dist

#################################################################################
# Build pex                                                                     #
#################################################################################
build: clean
	mkdir -p dist
	pex . -v --disable-cache -r requirements.txt -D emr_ingestion/docs -o dist/emr_ingestion.pex --python=$(PYTHON)

#################################################################################
# Run pex                                                                     #
#################################################################################
run:
	$(PYTHON) dist/emr_ingestion.pex -m emr_ingestion.main


#################################################################################
# Copy to s3 prod					                                            #
#################################################################################
tos3:
	@read -p "Enter AWS profile name [default]:" PROFILE; \
	aws s3 cp out/emr_ingestion.pex s3://emr-scripts-pex/runner/prod/ --profile $${PROFILE:-default}

#################################################################################
# Copy to s3 prod					                                            #
#################################################################################
tos3dev:
	@read -p "Enter AWS profile name [default]:" PROFILE; \
	aws s3 cp out/emr_ingestion.pex s3://emr-scripts-pex/runner/dev/ --profile $${PROFILE:-default}

#################################################################################
# Execute the build Docker                                                      #
#################################################################################
docker:
	rm -rf out
	docker build . --ssh github=$(SSH_KEYS) --target=result --output out

#################################################################################
# Deploy to prodution		                                                    #
#################################################################################
deploy-prod: docker tos3

#################################################################################
# Deploy to dev		                                                    		#
#################################################################################
deploy-dev: docker tos3dev
