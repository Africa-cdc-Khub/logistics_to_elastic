<?php
require __DIR__ . '/vendor/autoload.php';  // Ensure this path is correct

use Elastic\Elasticsearch\ClientBuilder;
use Dotenv\Dotenv;

// Load environment variables from .env file
$dotenv = Dotenv::createImmutable(__DIR__);
$dotenv->load();

// Set up MySQL connection using environment variables
$mysqli = new mysqli($_ENV['DB_HOST'], $_ENV['DB_USER'], $_ENV['DB_PASSWORD'], $_ENV['DB_NAME']);

if ($mysqli->connect_error) {
    die('Connect Error (' . $mysqli->connect_errno . ') ' . $mysqli->connect_error);
}
echo "Connected to MySQL\n";

// Set up Elasticsearch client with disabled SSL verification
$esClient = ClientBuilder::create()
    ->setHosts([$_ENV['ES_HOST']])
    ->setSSLVerification(false)
    ->setBasicAuthentication($_ENV['ES_USERNAME'], $_ENV['ES_PASSWORD'])
    ->build();

// Function to delete the index if it exists
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
                    'pledge_date' => ['type' => 'date', 'format' => 'yyyy-MM-dd'],  // Date format
                    'event_name' => ['type' => 'keyword'],  // Keyword for event name
                    'pledge_type' => ['type' => 'keyword'],  // Pledge type
                    'description' => ['type' => 'keyword'],  // Full-text search for description
                    'source_name' => ['type' => 'keyword'],  // Source name as keyword
                    'destination' => ['type' => 'keyword'],  // Destination
                    'item_name' => ['type' => 'keyword'],  // Item name
                    'quantity' => ['type' => 'float'],  // Float for quantity
                    'unit_price' => ['type' => 'float'],  // Float for unit price
                    'unit_weight' => ['type' => 'float'],  // Float for unit weight
                    'currency_code' => ['type' => 'keyword'],  // Currency code as keyword
                    'total_cost' => ['type' => 'float'],  // Float for total cost
                    'status' => ['type' => 'keyword'],  // Keyword for status
                    'estimated_delivery' => ['type' => 'text'],  // Text for estimated delivery
                    'unit_of_measure' => ['type' => 'keyword']  // Unit of measure as keyword
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
    $query = "SELECT * FROM pledge_visualization";  // Query your view
    $result = $mysqli->query($query);

    if ($result === false) {
        die('MySQL query error: ' . $mysqli->error);
    }

    while ($row = $result->fetch_assoc()) {
        try {
           // print_r($row);

           // exit();
            // Index data into Elasticsearch
            $esClient->index([
                'index' => $indexName,
                'body' => [
                    'pledge_date' => $row['pledge_date'],  // Date field
                    'event_name' => $row['event_name'],  // Event name
                    'pledge_type' => $row['pledge_type'],  // Pledge type
                    'description' => $row['description'],  // Description
                    'source_name' => $row['source_name'],  // Source name
                    'destination' => $row['destination'],  // Destination
                    'item_name' => $row['item_name'],  // Item name
                    'quantity' => (float) $row['quantity'],  // Quantity as float
                    'unit_price' => (float) $row['unit_price'],  // Unit price as float
                    'unit_weight' => (float) $row['unit_weight'],  // Unit weight as float
                    'currency_code' => $row['currency_code'],  // Currency code
                    'total_cost' => (float) $row['total_cost'],  // Total cost as float
                    'status' => $row['status'],  // Pledge status
                    'estimated_delivery' => $row['estimated_delivery'],  // Estimated delivery
                    'unit_of_measure' => $row['unit_of_measure'],  // Unit of measure
                ]
            ]);

         

            echo "Data indexed successfully\n";
        } catch (Exception $e) {
            echo 'Error indexing data: ' . $e->getMessage() . "\n";
        }
    }
}

// Define index name
$indexName = 'pledge_visualization_index';

// Delete the existing index if it exists
deleteIndex($esClient, $indexName);

// Create a new index with the correct mappings
createIndex($esClient, $indexName);

// Load data from the MySQL view into Elasticsearch
loadDataToElasticsearch($mysqli, $esClient, $indexName);

// Optionally, you can set up a cron job to run this script at intervals
// e.g., 0 */3 * * * php /path/to/your/script.php
?>