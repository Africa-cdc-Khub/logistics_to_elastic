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

// Function to create the index with proper mapping for numeric fields
function createIndex($esClient, $indexName)
{
    $params = [
        'index' => $indexName,
        'body' => [
            'mappings' => [
                'properties' => [
                    'event_name' => ['type' => 'text'],
                    'member_state' => ['type' => 'text'],
                    'item' => [
                        'properties' => [
                            'name' => ['type' => 'text'],
                            'category' => ['type' => 'text'],
                            'quantity' => ['type' => 'integer'],  // Ensure integer type for quantity
                            'unit_price' => ['type' => 'float'],  // Ensure float type for unit price
                            'total_cost' => ['type' => 'float'],  // Ensure float type for total cost
                        ],
                    ],
                    'logistics' => [
                        'properties' => [
                            'weight_kg' => ['type' => 'float'],  // Ensure float type for weight in kg
                            'weight_ton' => ['type' => 'float'],  // Ensure float type for weight in tons
                            'volume_cbm' => ['type' => 'float'],  // Ensure float type for volume in cbm
                            'delivery_date' => ['type' => 'date'],  // Ensure date type for delivery date
                        ],
                    ],
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

// Function to load data into Elasticsearch
function loadDataToElasticsearch($mysqli, $esClient, $indexName)
{
    $query = "SELECT * FROM combined";  // Use your actual table/view name
    $result = $mysqli->query($query);

    if ($result === false) {
        die('MySQL query error: ' . $mysqli->error);
    }

    while ($row = $result->fetch_assoc()) {
        try {
            $esClient->index([
                'index' => $indexName,
                'body' => [
                    'event_name' => $row['event'],  // Event name
                    'member_state' => $row['member_state'],  // Country or organization involved
                    'item' => [
                        'name' => $row['item_name'],  // Name of the item
                        'category' => $row['category_name'],  // Category of the item
                        'quantity' => (int) $row['qty'],  // Ensure integer type
                        'unit_price' => (float) $row['unit_price'],  // Ensure float type
                        'total_cost' => (float) $row['total_cost'],  // Ensure float type
                    ],
                    'logistics' => [
                        'weight_kg' => (float) $row['total_weight_kg'],  // Ensure float type
                        'weight_ton' => (float) $row['total_weight_ton'],  // Ensure float type
                        'volume_cbm' => (float) $row['volume_cbm'],  // Ensure float type
                        'delivery_date' => $row['delivery_date'],  // Ensure date type (assumes proper format in MySQL)
                    ]
                ]
            ]);

            echo "Data indexed successfully\n";
        } catch (Exception $e) {
            echo 'Error indexing data: ' . $e->getMessage() . "\n";
        }
    }
}

// Define index name
$indexName = 'africa_cdc_logistics';

// Delete the existing index
deleteIndex($esClient, $indexName);

// Create a new index with the correct data types
createIndex($esClient, $indexName);

// Load data into the newly created index
loadDataToElasticsearch($mysqli, $esClient, $indexName);

// Optionally, you can set up a cron job to run this script every 3 hours by configuring the cron job:
// 0 */3 * * * php /path/to/your/script.php
