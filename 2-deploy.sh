#!/bin/bash
set -eo pipefail
ARTIFACT_BUCKET=$(cat bucket-name.txt)
TEMPLATE=template-mvn.yml
mvn package
aws cloudformation package --template-file $TEMPLATE --s3-bucket $ARTIFACT_BUCKET --output-template-file out.yml
aws cloudformation deploy --template-file out.yml --stack-name s3-java --capabilities CAPABILITY_NAMED_IAM
