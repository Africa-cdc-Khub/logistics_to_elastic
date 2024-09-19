<?php
require 'vendor/autoload.php';

use Elasticsearch\ClientBuilder;
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

// Set up Elasticsearch client using environment variables
$esClient = ClientBuilder::create()->setHosts([$_ENV['ES_HOST']])->build();

// Function to load data into Elasticsearch
function loadDataToElasticsearch($mysqli, $esClient)
{
    $query = "SELECT * FROM combined_table_view";  // Use your actual table/view name
    $result = $mysqli->query($query);

    if ($result === false) {
        die('MySQL query error: ' . $mysqli->error);
    }

    while ($row = $result->fetch_assoc()) {
        try {
            $esClient->index([
                'index' => 'africa_cdc_logistics',
                'body' => [
                    'event_name' => $row['event'],  // Event name
                    'member_state' => $row['member_state'],  // Country or organization involved
                    'item' => [
                        'name' => $row['item_name'],  // Name of the item
                        'category' => $row['category_name'],  // Category of the item
                        'quantity' => $row['qty'],  // Quantity of items
                        'unit_price' => $row['unit_price'],  // Price per unit
                        'total_cost' => $row['total_cost'],  // Total cost
                    ],
                    'logistics' => [
                        'weight_kg' => $row['total_weight_kg'],  // Total weight in kg
                        'weight_ton' => $row['total_weight_ton'],  // Total weight in tons
                        'volume_cbm' => $row['volume_cbm'],  // Volume in cubic meters
                        'delivery_date' => $row['delivery_date'],  // Date of delivery
                    ]
                ]
            ]);

            echo "Data indexed successfully\n";
        } catch (Exception $e) {
            echo 'Error indexing data: ' . $e->getMessage() . "\n";
        }
    }
}

// Run the function manually or on schedule
loadDataToElasticsearch($mysqli, $esClient);

// Optionally, you can set up a cron job to run this script every 3 hours by configuring the cron job:
// 0 */3 * * * php /path/to/your/script.php
?>