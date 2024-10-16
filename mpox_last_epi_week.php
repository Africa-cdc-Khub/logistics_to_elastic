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
                    
                    'organisationunitname' => ['type' => 'keyword'],
                    'periodid' => ['type' => 'keyword'],
                    'total_suspected_cases' => ['type' => 'integer'],
                    'total_confirmed_cases' => ['type' => 'integer'],
                    'total_deaths' => ['type' => 'integer'],
                    'week' => ['type' => 'keyword'],
                 
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
    $query = "SELECT * FROM new_cases_lastweek";  // Query your view
    $result = $mysqli->query($query);

    if ($result === false) {
        die('MySQL query error: ' . $mysqli->error);
    }

    while ($row = $result->fetch_assoc()) {
        try {
            // Index data into Elasticsearch
            $cfr = 0;
            $suspectedCases = intval($row['Suspected Mpox Cases']);
            $deaths = intval($row['Mpox Deaths']);

            if ($suspectedCases > 0) {
                $cfr = ($deaths / $suspectedCases) * 100;
            }
            $esClient->index([
                'index' => $indexName,
                'body' => [
                    'organisationunitname' => $row['organisationunitname'],
                    'periodid' => $row['periodid'],
                    'total_suspected_cases' => $row['total_suspected_cases'],
                    'total_confirmed_cases' => $row['total_confirmed_cases'],
                    'total_deaths' => $row['total_deaths'],
                    'week' => $row['week'],
                 
                ]
            ]);

            echo "Data indexed successfully\n";
        } catch (Exception $e) {
            echo 'Error indexing data: ' . $e->getMessage() . "\n";
        }
    }
}

// Define index name
$indexName = 'new_cases_lastweek';

// Delete the existing index if it exists
deleteIndex($esClient, $indexName);

// Create a new index with the correct mappings
createIndex($esClient, $indexName);

// Load data from the MySQL view into Elasticsearch
loadDataToElasticsearch($mysqli, $esClient, $indexName);

// Optionally, you can set up a cron job to run this script at intervals
// e.g., 0 */3 * * * php /path/to/your/script.php
?>