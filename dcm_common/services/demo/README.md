# DCM Demo Service
An implementation of a generic DCM service.

Intended use:
* demonstration of new features,
* tests, and
* reference/documentation.


## Building SDK
Download OpenAPITools
```
wget https://repo1.maven.org/maven2/org/openapitools/openapi-generator-cli/7.5.0/openapi-generator-cli-7.5.0.jar -O openapi-generator-cli.jar
```

Run on `openapi.yaml` with `sdk.json`
```
java -jar <path-to-openapi-generator-cli.jar> generate -i dcm_common/services/demo/openapi.yaml -g python -c dcm_common/services/demo/sdk.json -o dcm-demo-sdk
```

Install as regular python package
```
pip install dcm-demo-sdk/
```
