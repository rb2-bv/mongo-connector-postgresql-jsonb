Feature: Mongo Postgres Jsonb Connector
  Core functionality of jsonb connector CRUD Operations

  Background: MongoDB and Postgres are running
    Given mongodb and postgres are running
    And there is an empty postgres table named 'collection1'
    And there is an empty mongo collection 'database.collection1'
    And the jsonb connector is running targeting the mongo collection 'database.collection1'


  Scenario: Document Inserts
    Given the jsonb connector is running
    When a document is inserted into the mongo collection
    Then the document is inserted into to the collection1 table in postgres

  Scenario: Document Removals
    Given a document exists in mongodb and postgres
    When the document is deleted from the mongo collection
    Then the document is deleted from the collection1 table in postgres

  Scenario: Field Updates
    Given a document exists in mongodb and postgres
    When a field in the document is updated in mongo
    Then the document in postgres reflects the update

  Scenario: Field Deletions
    Given a document exists in mongodb and postgres
    When a field in the document is unset in mongo
    Then the document in postgres reflects the unset

  Scenario: Nested Value Deletions
    Given a document exists with a nested value in mongodb and postgres
    When a nested field in the document is unset in mongo
    Then the nested field is removed in postgres

  Scenario: Nested Value Updates
    Given a document exists with a nested value in mongodb and postgres
    When a nested field in the document is updated in mongo
    Then the nested field is updated in postgres
