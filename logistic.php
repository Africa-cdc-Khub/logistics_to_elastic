<?php
include('modules/connection.php');
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

// Function to create the index with proper mapping for keywords, numeric fields, and the source field
function createIndex($esClient, $indexName)
{
    $params = [
        'index' => $indexName,
        'body' => [
            'mappings' => [
                'properties' => [
                    'event_name' => ['type' => 'keyword'],  // Changed to keyword
                    'member_state' => ['type' => 'keyword'],  // Changed to keyword
                    'source' => ['type' => 'keyword'],  // Added source as a keyword
                    'item' => [
                        'properties' => [
                            'name' => ['type' => 'keyword'],  // Changed to keyword
                            'category' => ['type' => 'keyword'],  // Changed to keyword
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
                    'source' => $row['source'],  // Source field as keyword
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

// Create a new index with the correct data types and source as a keyword
createIndex($esClient, $indexName);

// Load data into the newly created index
loadDataToElasticsearch($mysqli, $esClient, $indexName);

// Optionally, you can set up a cron job to run this script every 3 hours by configuring the cron job:
// 0 */3 * * * php /path/to/your/script.php
