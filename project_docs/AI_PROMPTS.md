# How AI Was Used in This Project

## Why this document exists

The project brief asks for an honest account of how AI assistance was used
across the development workflow — including the prompts that were given to the
AI, and a candid description of where AI fell short and needed manual
correction. This document provides that account in plain language for business
and non-technical readers.

## At a glance

| | |
|---|---|
| **AI tool used** | Claude Code (Anthropic) |
| **What it did** | Scaffolded the project's initial structure and layout |
| **How it was used** | A series of guided prompts, with a human reviewing and correcting the output |
| **Human oversight** | Every output was reviewed; three items needed manual intervention (see below) |

The initial structure of this project was scaffolded with **Claude Code**,
Anthropic's AI coding assistant. AI was used to bootstrap the project, wire up
the web framework and the API documentation, and produce the steps needed to
deploy the service — always with a person checking and correcting the result.

## The prompts

The prompts below are grouped by the stage of work they supported.

### 1. Bootstrapping the project

- Confirm the Go version.
- Initialise the Go module (`go mod init`).
- Tidy the project's dependencies (`go mod tidy`).
- Add the Gin web framework for handling HTTP requests.
- Set up the Swagger / OpenAPI documentation.
- Generate the router.

### 2. Routes and a first deployment check

> "Set up routes just for checking the deployment on minikube first."

### 3. API documentation, integration, and deployment

> "Generate swagger doc and integrate with the Go HTTPS APIs and generate
> deployment steps onto the minikube; bring up the minikube cluster if it
> doesn't exist."

## Where AI fell short

AI accelerated the early scaffolding, but its output was not accepted blindly.
As recorded in the project [README](../README.md), three items required manual
intervention:

1. **A real bug** — an "offset-0" defect that was caught by the generated tests.
2. **An OpenAPI correction** — the `swag init` command had to be corrected.
3. **A design change** — how the CSV telemetry data is delivered was redesigned
   by hand.

These corrections are the reason every AI-generated output in this project was
reviewed before being kept.
