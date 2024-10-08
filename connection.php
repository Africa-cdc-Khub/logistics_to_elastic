<?php

require __DIR__ . '/vendor/autoload.php';

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
