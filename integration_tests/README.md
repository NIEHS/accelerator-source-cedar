# Integration Tests

Unlike the /tests directory, this testing directory contains longer running tests that utilize components of the running
development system, including Mongo and potentially Airflow.

Normal unit tests belong in the /tests directory.These tests are not included in the normal post-commit Github actions.

## Configuration

see the application.properties file in this folder for guidance on environment variables that need to be set
to run these tests

```properties

# properties file for testing accelerator
api_key=xxxx # set env variable CEDAR_API_KEY
cedar_endpoint=https://resource.metadatacenter.org
chords_folder=xxxx # set the test folder target here as CEDAR_FOLDER_ID

```

