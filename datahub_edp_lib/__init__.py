from typing import List

import urllib3
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

urllib3.disable_warnings()


class DataHubGraphql:
    def __init__(self, base_url, token, use_ssl=False):
        self.base_url = base_url
        self.token = token
        self.request_header = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/json',
        }
        self.use_ssl = use_ssl

        self.transport = RequestsHTTPTransport(url=self.base_url, headers=self.request_header, verify=self.use_ssl)
        self.client = Client(transport=self.transport)

    def _get_ingestion_sources(self, start: int = 0, count: int = 100) -> list:
        """
        Lists all ingestion_sources.
        param: start: The offset of the result set
        param: count: The number of entities to include in result set
        return: Ingestion sources
        """

        query = """
query listIngestionSources($input: ListIngestionSourcesInput!) {
                  listIngestionSources(input: $input) {
                    start
                    count
                    total
                    ingestionSources {
                      urn
                      name
                      schedule {
                        interval
                        timezone
                      }
                      platform {
                        name
                      }
                      type
                      config {
                        version
                        executorId
                        recipe
                      }
                    }
                  }
                }
                """
        variables = {'input': {'start': start, 'count': count}}
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result['listIngestionSources']

    def get_container_entities(self, urn: str) -> dict:
        """
        Lists all container entities.
        param: urn: Uniform resource name: example urn:<Namespace>:<Entity Type>:<ID>
        return: Containers entities
        """

        query = """
                query list_container_entities($urn: String!) {
                    container(urn: $urn) {
                        entities {
                            start
                            count
                            total
                            searchResults {
                                entity {
                                    urn
                                    type
                                 }
                            }
                        }
                    }
                }
                """
        variables = {'urn': urn}
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def _search_container_entities(
        self,
        value: str,
        field: str = 'container',
        search_query: str = '',
        start: int = 0,
        count: int = 100,
    ) -> dict:
        """
        Lists all container entities.
        param: value: Value of the field to filter by, for e.g. full urn of container
        param: field: Entity field to be searched
        return: Containers entities
        """

        query = """
                query search_across_entities($input: SearchAcrossEntitiesInput!) {
                    searchAcrossEntities(input: $input) {
                        count
                        total
                        searchResults {
                            entity {
                                urn
                                type
                                ... on Container {
                                    properties {
                                        name
                                    }
                                    editableProperties {
                                        description
                                    }
                                }
                            }

                        }
                    }
                }
                """
        variables = {
            'input': {
                'types': 'CONTAINER',
                'query': search_query,
                'start': start,
                'count': count,
                'filters': [{'field': field, 'value': value}],
            },
        }
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def get_all_containers_urns(self, start: int = 0, count: int = 100) -> dict:
        """
        Lists all containers urns
        param: types: CONTAINER
        param: query: * - all possible containers
        param: start: The offset of the result set
        param: count: The number of entities to include in result set
        return: Containers urns
        """

        query = """
                query search_across_entities($input: SearchAcrossEntitiesInput!) {
                  searchAcrossEntities(input: $input) {
                    count
                    total
                    searchResults {
                      entity {
                        urn
                        type
                        ... on DataPlatform {
                          urn
                          type
                          properties {
                            displayName
                          }
                        }
                      }
                    }
                  }
                }
                """
        variables = {'input': {'types': 'CONTAINER', 'query': '*', 'start': start, 'count': count}}
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def get_dataset_fields(
        self,
        name: str,
        start: int = 0,
        count: int = 100,
    ) -> dict:
        """
        Get fields of the dataset entity.
        param: types: DATASET
        param: query: name of the dataset to search for
        param: start: The offset of the result set
        param: count: The number of entities to include in result set
        return: Dataset info
        """

        query = """
                query search_dataset_fields($input: SearchAcrossEntitiesInput!) {
                    searchAcrossEntities(input: $input) {
                        count
                        total
                        searchResults {
                            entity {
                                urn
                                type
                                ... on Dataset {
                                    properties {
                                        name
                                    }
                                    schemaMetadata {
                                      fields {
                                        fieldPath
                                      }
                                    }
                                }
                            }

                        }
                    }
                }
                """
        variables = {'input': {'types': 'DATASET', 'query': name, 'start': start, 'count': count}}
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def _search_container_entities_datasets(
        self,
        value: str,
        types: List[str] = None,
        field: str = 'container',
        search_query: str = '',
        start: int = 0,
        count: int = 100,
    ) -> dict:
        """
        Lists all container entities.
        param: value: Value of the field to filter by, for e.g. full urn of container
        param: types: Entity types to be searched https://datahubproject.io/docs/graphql/enums#entitytype
        param: field: Entity field to be searched
        return: Containers entities
        """

        query = """
                query search_across_entities($input: SearchAcrossEntitiesInput!) {
                    searchAcrossEntities(input: $input) {
                        count
                        total
                        searchResults {
                            entity {
                                urn
                                type
                                ... on Dataset {
                                    properties {
                                        name
                                    }
                                    editableProperties {
                                        description
                                    }
                                    schemaMetadata {
                                          fields {
                                            fieldPath
                                          }
                                    }
                                }
                            }

                        }
                    }
                }
                """
        variables = {
            'input': {
                'types': types,
                'query': search_query,
                'start': start,
                'count': count,
                'filters': [{'field': field, 'value': value}],
            },
        }
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def _search_entities(self, entity_type: str, search_query: str, start: int = 0, count: int = 100) -> dict:
        """
        Search entities by input type and query.
        param: entity_type: Entitie type, full list https://datahubproject.io/docs/graphql/enums#entitytype
        param: search_query: Query for search, for e.g "DWH"
        param: start: The offset of the result set
        param: count: The number of entities to include in result set
        return: Search results
        """

        query = """
                query search_entities($input: SearchInput!) {
                    search(input: $input) {
                        start
                        count
                        total
                        searchResults {
                            entity {
                                urn
                                type
                                ...on Dataset {
                                    name
                                }
                            }
                        }
                    }
                }
                """
        variables = {
            'input': {
                'type': entity_type,
                'query': search_query,
                'start': start,
                'count': count,
            }
        }
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def _update_container_description(self, urn: str, description: str) -> dict:
        """
        Update container description.
        param: urn: Uniform resource name: example urn:<Namespace>:<Entity Type>:<ID>
        param: description: Description of dataset
        return: Update status
        """
        query = """
                mutation updateDescription($description: String!, $urn: String!) {
                    updateDescription(input: {description: $description, resourceUrn: $urn}) {
                        editableProperties {
                            description
                        }
                    }
                }
                """

        variables = {'urn': urn, 'description': description}
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def _update_dataset_description(self, urn: str, description: str) -> dict:
        """
        Update dataset description.
        param: urn: Uniform resource name: example urn:<Namespace>:<Entity Type>:<ID>
        param: description: Description of dataset
        return: Update status
        """
        query = """
                mutation updateDataset($urn: String!, $input: DatasetUpdateInput!) {
                    updateDataset(urn: $urn, input: $input) {
                        editableProperties {
                            description
                        }
                    }
                }
                """

        variables = {
            'urn': urn,
            'input': {'editableProperties': {'description': description}},
        }
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def _get_dataset_custom_properties(self, urn: str) -> dict:
        """
        Get custom properties of a Dataset
        :param urn: Uniform resource name: example urn:<Namespace>:<Entity Type>:<ID>
        :return: Dataset urn, name and its custom properties
        """
        query = """
                query get_dataset_props($urn: String!) {
                    dataset(urn: $urn) {
                    urn
                    name
                    properties {
                        customProperties {
                            key
                            value
                            associatedUrn
                        }
                    }
                  }
                }
                """
        variables = {'urn': urn}
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def _get_dataset_tags(self, urn: str) -> dict:
        """
        Get tags of a Dataset
        :param urn: Uniform resource name: example urn:<Namespace>:<Entity Type>:<ID>
        :return: Dataset urn, name and its tags
        """
        query = """
            query get_dataset_tags($urn: String!) {
                dataset(urn: $urn) {
                urn
                name
                tags {
                    tags {
                        tag {
                            urn
                            name
                            description
                        }
                    }
                }
              }
            }
        """
        variables = {'urn': urn}
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def create_tag(self, tag_name: str, description: str) -> dict:
        """
        Create a tag
        :param tag_name: The name of the tag
        :param description: A short description of the tag
        :return: Created tag's urn
        """
        query = """
                mutation create_tag($name: String!, $description: String!) {
                    createTag(input: {id: $name, name: $name, description: $description})
                }
                """
        variables = {'name': tag_name, 'description': description}
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def search_for_tag(self, tag_urn: str) -> dict:
        """
        Search for a tag
        :param tag_urn: The urn of the tag
        :return: Name of a tag
        """
        query = """
                query list_tag_entities($urn: String!) {
                    tag(urn: $urn) {
                        properties {
                          name
                        }
                      }
                    }
                """
        variables = {'urn': tag_urn}
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def delete_tag(self, urn: str) -> dict:
        """
        Delete the tag
        :param urn: Uniform resource name: example urn:<Namespace>:<Entity Type>:<ID>
        :return: Confirmation of tag removal
        """
        query = """
                mutation delete_tag($urn: String!) {
                    deleteTag(urn: $urn)
                }
                """
        variables = {'urn': urn}
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def add_tag(self, tag_urn: str, resource_urn: str) -> dict:
        """
        Add the existing tag to the resource
        :param tag_urn: Uniform resource name: example urn:<Namespace>:<Entity Type>:<ID>
        :param resource_urn: Uniform resource name: example urn:<Namespace>:<Entity Type>:<ID>
        :return: Confirmation of adding the tag to the dataset
        #"""
        return self._add_or_remove_tags(
            tag_urns=[tag_urn],
            resource_urns=[resource_urn],
            datahub_method='batchAddTags',
        )

    def remove_tag(self, tag_urn: str, resource_urn: str) -> dict:
        """
        Remove the tag from the resource
        :param tag_urn: Uniform resource name: example urn:<Namespace>:<Entity Type>:<ID>
        :param resource_urn: Uniform resource name: example urn:<Namespace>:<Entity Type>:<ID>
        :return: Confirmation of the tag removal from the resource
        """
        return self._add_or_remove_tags([tag_urn], [resource_urn], 'batchRemoveTags')

    def batch_add_tags(self, tag_urns: List[str], resource_urns: List[str]) -> dict:
        """
        Add tags to multiple Entities or subresources
        :param tag_urns: Uniform resource names: example urn:<Namespace>:<Entity Type>:<ID>
        :param resource_urns: Uniform resource names: example urn:<Namespace>:<Entity Type>:<ID>
        :return: Confirmation of adding the tags to the resource
        """
        return self._add_or_remove_tags(tag_urns, resource_urns, 'batchAddTags')

    def batch_remove_tags(self, tag_urns: List[str], resource_urns: List[str]) -> dict:
        """
        Add the existing tag to the dataset
        :param tag_urns: Uniform resource names: example urn:<Namespace>:<Entity Type>:<ID>
        :param resource_urns: Uniform resource names: example urn:<Namespace>:<Entity Type>:<ID>
        :return: Confirmation of the tags' removal from the resource
        """
        return self._add_or_remove_tags(tag_urns, resource_urns, 'batchRemoveTags')

    def _add_or_remove_tags(self, tag_urns: List[str], resource_urns: List[str], datahub_method: str) -> dict:
        query = (
            """
                mutation add_or_remove_tags($tagUrns: [String!]!, $resources: [ResourceRefInput!]!) {
                    %s(input: {tagUrns: $tagUrns, resources: $resources})
                }
                """
            % datahub_method
        )
        variables = {
            'tagUrns': tag_urns,
            'resources': [{'resourceUrn': urn} for urn in resource_urns],
        }
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def add_field_tag(self, tag_urn: str, resource_urn: str, subresource: str) -> dict:
        """
        Add the existing tag to the resource
        :param tag_urn: Uniform resource name: example urn:<Namespace>:<Entity Type>:<ID>
        :param resource_urn: Uniform resource name: example urn:<Namespace>:<Entity Type>:<ID>
        :param subresource: Name of the field of dataset
        :return: Confirmation of adding the tag to the dataset field
        #"""
        return self._add_or_remove_field_tags([tag_urn], resource_urn, subresource, 'addTags')

    def remove_field_tag(self, tag_urn: str, resource_urn: str, subresource: str) -> dict:
        """
        Remove the tag from the resource
        :param tag_urn: Uniform resource name: example urn:<Namespace>:<Entity Type>:<ID>
        :param resource_urn: Uniform resource name: example urn:<Namespace>:<Entity Type>:<ID>
        :param subresource: Name of the field of dataset
        :return: Confirmation of the tag removal from the resource field
        """
        return self._add_or_remove_field_tags([tag_urn], resource_urn, subresource, 'removeTag')

    def _add_or_remove_field_tags(
        self, tag_urns: List[str], resource_urn: str, subresource: str, datahub_method: str
    ) -> dict:
        query = (
            """
                  mutation add_or_remove_tags($tagUrns: [String!]!, $resourceUrn: String!, $subResource: String!) {
                      %s(input: {tagUrns: $tagUrns, resourceUrn: $resourceUrn,
                                    subResourceType:DATASET_FIELD, subResource:$subResource})
                  }
                """
            % datahub_method
        )
        variables = {'tagUrns': tag_urns, 'resourceUrn': resource_urn, 'subResource': subresource}
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def update_ingestion_recipe(
        self,
        urn: str,
        ingestion_name: str,
        platform_type: str,
        schedule: dict,
        executor_id: str,
        version: str,
        recipe: str,
    ) -> dict:
        """
        Add the existing tag to the resource
        :param urn: Uniform ingestion name.
        :param name: Name of ingestion.
        :param type: Type of ingestion.
        :param schedule: Schedule of ingestion: should be None or like {"interval": "", "timezone": ""}.
        :param executorId: executorId of ingestion.
        :param version: Version of ingestion.
        :param recipe: Full recipe of ingestion.
        :return: Confirmation of updating a recipe
        #"""
        query = """
                  mutation UpdateIngestionSourceInput($urn: String!, $config: UpdateIngestionSourceInput!) {
                      updateIngestionSource(urn: $urn, input: $config)
                    }
                """
        variables = {
            'urn': urn,
            'config': {
                'name': ingestion_name,
                'type': platform_type,
                'schedule': schedule,
                'config': {'executorId': executor_id, 'version': version, 'recipe': recipe},
            },
        }
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def get_kafka_topics(
        self,
        environment: str,
        search_query: str = '*',
        start: int = 0,
        count: int = 100,
    ) -> dict:
        """
        Get kafka topics for specified environment
        :param environment: FabricType (https://datahubproject.io/docs/graphql/enums/#fabrictype)
        :param search_query: Query for search, for e.g. "smi". "*" is default value for all topics
        :param start: The offset of the result set
        :param count: The number of entities to include in result set
        :return: Search result (Kafka dataset information: resource urn, topic name, resource tags)
        """
        query = """
                query get_kafka_topics($environment: String!, $query: String!, $start: Int, $count: Int) {
                    search(input: {
                            type: DATASET,
                            query: $query,
                            count: $count,
                            start: $start,
                            orFilters: [{
                                and: [{
                                    field: "platform",
                                    values: ["urn:li:dataPlatform:kafka"]
                                }, {
                                    field: "origin",
                                    values: [$environment]
                                }]
                            }]
                    }) {
                        total
                        searchResults {
                            entity {
                                ... on Dataset {
                                    urn
                                    name
                                    properties {
                                        name
                                    }
                                    tags {
                                        tags {
                                            tag {
                                                urn
                                                name
                                            }
                                            associatedUrn
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
        """
        variables = {
            'environment': environment,
            'query': search_query,
            'start': start,
            'count': count,
        }
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def get_kafka_topic_by_name(
        self,
        environment: str,
        topic_name: str = '',
        start: int = 0,
        count: int = 100,
    ) -> dict:
        """
        Get kafka topics for specified environment
        :param environment: FabricType (https://datahubproject.io/docs/graphql/enums/#fabrictype)
        :param topic_name: Name of a kafka topic
        :param start: The offset of the result set
        :param count: The number of entities to include in result set
        :return: Search result (Kafka dataset information: resource urn, topic name, resource tags)
        """
        query = """
                query get_kafka_topics($environment: String!, $topicName: String!, $start: Int, $count: Int) {
                    search(input: {
                            type: DATASET,
                            query: $topicName,
                            count: $count,
                            start: $start,
                            orFilters: [{
                                and: [{
                                    field: "platform",
                                    values: ["urn:li:dataPlatform:kafka"]
                                }, {
                                    field: "origin",
                                    values: [$environment]
                                }, {
                                    field: "name",
                                    values: [$topicName]
                                }]
                            }]
                    }) {
                        total
                        searchResults {
                            entity {
                                ... on Dataset {
                                    urn
                                    name
                                    properties {
                                        name
                                    }
                                    tags {
                                        tags {
                                            tag {
                                                urn
                                                name
                                            }
                                            associatedUrn
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
        """
        variables = {
            'environment': environment,
            'query': topic_name,
            'topicName': topic_name,
            'start': start,
            'count': count,
        }
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def create_secret_input(self, name: str, value: str, description: str):
        """
        Creating a new Secret
        :param name: Secret Name
        :param value: Secret value
        :param description: Description that the user see
        """
        query = """ mutation CreateSecret($name: String!, $value: String!, $description: String!) {
                            createSecret(input: {name: $name, value: $value, description: $description})
                        }
                        """

        variables = {'name': name, 'value': value, 'description': description}
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result

    def create_ingestion(
        self,
        name: str,
        db_type: str,
        description: str,
        cron_minutes: str,
        cron_hours: str,
        platform_instance: str,
        database_name: str,
        password: str,
        host_port: str,
        username: str,
        pipeline_name: str,
        owner_urns: str,
        tag_urns: str,
    ):
        """
        Creating a new Ingestion
        :param name: Ingestion name
        :param db_type: Type of database
        :param description: Info about ingestion
        :param cron_minutes: Minutes that are put in the schedule
        :param cron_hours: Hours that are put in the schedule
        :param platform_instance: platform - prod, test, uat
        :param database_name: Database name
        :param password: Password for connect to database
        :param host_port: Hostport for connect to database
        :param username: Username for connect to database
        :param pipeline_name: Pipeline name it is database hostname
        :param owner_urns: Owner urns it is product_owner
        :param tag_urns: Tag urns it is product_name or product_name_cmdb
        """

        query = f"""mutation CreateIngestion($name: String!, $db_type: String!, $description: String!)
        {{ createIngestionSource(input: {{
      name: $name,
      type: $db_type,
      description: $description,
      schedule: {{interval: "{cron_minutes} {cron_hours} * * *", timezone: "UTC+03:00"}},
      config: {{
         recipe: "{{\\"source\\":{{\\"type\\":\\"postgres\\",\\"config\\":{{\\"stateful_ingestion\\":{{\\"enabled\\": true,\\"remove_stale_metadata\\":\\"true\\"}},\\"platform_instance\\":\\"{platform_instance}\\",\\"include_tables\\":true,\\"database\\":\\"{database_name}\\",\\"password\\":\\"{password}\\",\\"profiling\\":{{\\"enabled\\":false}},\\"host_port\\":\\"{host_port}\\",\\"include_views\\":true,\\"username\\":\\"{username}\\"}}}},\\"pipeline_name\\":\\"{pipeline_name}\\",\\"datahub_api\\":{{\\"server\\":\\"http://datahub-gms-datahub-gms:8080\\"}},\\"sink\\":{{\\"type\\":\\"datahub-rest\\",\\"config\\":{{\\"server\\":\\"http://datahub-gms-datahub-gms:8080\\",\\"token\\":\\"${{ingestions_by_yua}}\\"}}}},\\"transformers\\":[{{\\"type\\":\\"simple_add_dataset_ownership\\",\\"config\\":{{\\"semantics\\":\\"PATCH\\",\\"owner_urns\\":[\\"{owner_urns}\\"],\\"ownership_type\\":\\"PRODUCER\\" }}}},{{\\"type\\":\\"simple_add_dataset_tags\\",\\"config\\":{{\\"semantics\\":\\"PATCH\\",\\"tag_urns\\":[\\"{tag_urns}\\"]}}}}]}}",
         executorId: "default",
            }}
        }})
    }}"""

        variables = {'name': name, 'type': db_type, 'description': description}
        query = gql(query)
        result = self.client.execute(query, variable_values=variables)
        return result
