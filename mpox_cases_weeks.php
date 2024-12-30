<?php
include('connection_dashboard.php');

function deleteIndex($esClient, $indexName)
{
    try {
        $exists = $esClient->indices()->exists(['index' => $indexName]);

        if ($exists->asBool()) {
            $esClient->indices()->delete(['index' => $indexName]);
            echo "Index '$indexName' deleted successfully.\n";
        } else {
            echo "Index '$indexName' does not exist.\n";
        }
    } catch (Exception $e) {
        echo 'Error deleting index: ' . $e->getMessage() . "\n";
    }
}

// Function to create the index with proper mapping
function createIndex($esClient, $indexName)
{
    $params = [
        'index' => $indexName,
        'body' => [
            'mappings' => [
                'properties' => [
                    'organisationunitid' => ['type' => 'keyword'],
                    'organisationunitname' => ['type' => 'keyword'],
                    'organisationunitcode' => ['type' => 'keyword'],
                    'periodid' => ['type' => 'keyword'],
                    'periodname' => ['type' => 'keyword'],
                    'periodcode' => ['type' => 'keyword'],
                    'confirmed_mpox_cases' => ['type' => 'integer'],
                    'mpox_deaths' => ['type' => 'integer'],
                    'suspected_mpox_cases' => ['type' => 'integer'],
                    'iso2_code' => ['type' => 'keyword'],
                    'iso3_code' => ['type' => 'keyword'],
                    'longitude' => ['type' => 'float'],
                    'latitude' => ['type' => 'float'],
                    'location' => ['type' => 'geo_point'],
                    'population' => ['type' => 'integer'],
                    'region_name' => ['type' => 'keyword'],
                    'member_state' => ['type' => 'keyword'],
                    'member_state_id' => ['type' => 'keyword'],
                    'cfr' => ['type' => 'float'],
                ],
            ],
        ],
    ];

    try {
        $esClient->indices()->create($params);
        echo "Index '$indexName' created successfully with proper mappings.\n";
    } catch (Exception $e) {
        echo 'Error creating index: ' . $e->getMessage() . "\n";
    }
}

// Function to load data from MySQL to Elasticsearch
function loadDataToElasticsearch($mysqli, $esClient, $indexName)
{
    $query = "SELECT * FROM mpox_cases_by_week";  // Query your view
    $result = $mysqli->query($query);

    if ($result === false) {
        die('MySQL query error: ' . $mysqli->error);
    }

    while ($row = $result->fetch_assoc()) {
        try {
            // Index data into Elasticsearch
            $cfr = 0;
            $suspectedCases = intval($row['Suspected_Mpox_Cases']);
            $deaths = intval($row['Mpox_Deaths']);

            if ($suspectedCases > 0) {
                $cfr = ($deaths / $suspectedCases) * 100;
            }
            $esClient->index([
                'index' => $indexName,
                'body' => [
                    'organisationunitid' => $row['organisationunitid'],
                    'organisationunitname' => $row['organisationunitname'],
                    'organisationunitcode' => $row['organisationunitcode'],
                    'periodid' => $row['periodid'],
                    'periodname' => $row['periodname'],
                    'periodcode' => $row['periodcode'],
                    'Confirmed Mpox Cases' => (int) $row['Confirmed_Mpox_Cases'],
                    'Mpox Deaths' => (int) $row['Mpox_Deaths'],
                    'Suspected Mpox Cases' => (int) $row['Suspected_Mpox_Cases'],
                    'iso2_code' => $row['iso2_code'],
                    'iso3_code' => $row['iso3_code'],
                    'longitude' => (float) $row['longitude'],
                    'latitude' => (float) $row['latitude'],
                    'location' => [
                        'lat' => (float) $row['latitude'],
                        'lon' => (float) $row['longitude']
                    ],  // Geo-point field
                    'population' => (int) $row['population'],
                    'region_name' => $row['region_name'],
                    'member_state' => $row['member_state_id'],
                    'member_state_id' => $row['member_state_id'],
                    'cfr' => $cfr,
                ]
            ]);

            echo "Data indexed successfully\n";
        } catch (Exception $e) {
            echo 'Error indexing data: ' . $e->getMessage() . "\n";
        }
    }
}

// Define index name
$indexName = 'mpox_cases_by_weeks_index';

// Delete the existing index if it exists
deleteIndex($esClient, $indexName);

// Create a new index with the correct mappings
createIndex($esClient, $indexName);

// Load data from the MySQL view into Elasticsearch
loadDataToElasticsearch($mysqli, $esClient, $indexName);

// Optionally, you can set up a cron job to run this script at intervals
// e.g., 0 */3 * * * php /path/to/your/script.php
?>