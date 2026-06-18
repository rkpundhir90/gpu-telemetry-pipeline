.PHONY: openapi

# Regenerate the OpenAPI/Swagger spec from the handler annotations into docs/.
# The general API info lives on NewRouter in internal/api/router.go, so point
# swag at it explicitly and parse internal packages for the model schemas.
openapi:
	swag init \
		--generalInfo internal/api/router.go \
		--output docs \
		--parseInternal \
		--parseDependency
