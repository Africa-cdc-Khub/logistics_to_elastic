<?php
// upload.php

require __DIR__ . '/vendor/autoload.php';  // Ensure this path is correct

use Elastic\Elasticsearch\ClientBuilder;
use Dotenv\Dotenv;

// Load environment variables from .env file
$dotenv = Dotenv::createImmutable(__DIR__);
$dotenv->load();

// Set up Elasticsearch client with disabled SSL verification
$esClient = ClientBuilder::create()
    ->setHosts([$_ENV['ES_HOST']])
    ->setSSLVerification(false)
    ->setBasicAuthentication($_ENV['ES_USERNAME'], $_ENV['ES_PASSWORD'])
    ->build();

// Check if a file was uploaded
if (isset($_FILES['datafile']) && $_FILES['datafile']['error'] == 0) {
    $fileTmpPath = $_FILES['datafile']['tmp_name'];
    $fileName = $_FILES['datafile']['name'];
    $fileSize = $_FILES['datafile']['size'];
    $fileType = $_FILES['datafile']['type'];
    $fileNameCmps = explode(".", $fileName);
    $fileExtension = strtolower(end($fileNameCmps));

    $allowedfileExtensions = array('csv', 'xml', 'xls', 'xlsx');

    if (in_array($fileExtension, $allowedfileExtensions)) {
        // Process the uploaded file

        // Get index name from POST data
        $indexName = $_POST['index_name'];

        // Read data from the file and parse it
        $data = [];

        if ($fileExtension == 'csv') {
            $data = parseCSV($fileTmpPath);
        } elseif ($fileExtension == 'xml') {
            $data = parseXML($fileTmpPath);
        } elseif ($fileExtension == 'xls' || $fileExtension == 'xlsx') {
            $data = parseExcel($fileTmpPath);
        } else {
            die('Unsupported file type.');
        }

        if (empty($data)) {
            die('No data found in the file.');
        }

        // Determine data types and build Elasticsearch mapping
        $mapping = buildMapping($data);

        // Delete the existing index if it exists
        deleteIndex($esClient, $indexName);

        // Create a new index with the generated mapping
        createIndex($esClient, $indexName, $mapping);

        // Index the data into Elasticsearch
        indexData($esClient, $indexName, $data);

        echo "Data successfully uploaded and indexed into Elasticsearch.";
    } else {
        die('Unsupported file extension.');
    }
} else {
    die('No file uploaded or there was an upload error.');
}

// Function to parse CSV files
function parseCSV($filePath)
{
    $data = [];
    if (($handle = fopen($filePath, 'r')) !== false) {
        $header = null;
        while (($row = fgetcsv($handle, 1000, ',')) !== false) {
            if (!$header) {
                $header = $row;
            } else {
                if (count($header) != count($row)) {
                    // Skip or handle error
                    continue;
                }
                $data[] = array_combine($header, $row);
            }
        }
        fclose($handle);
    }
    return $data;
}

// Function to parse XML files
function parseXML($filePath)
{
    $data = [];
    $xml = simplexml_load_file($filePath, 'SimpleXMLElement', LIBXML_NOCDATA);
    $json = json_encode($xml);
    $array = json_decode($json, true);

    // Adjust the following line according to your XML structure
    $data = array_values($array)[0]; // Assuming the first element contains the data
    return $data;
}

// Function to parse Excel files
function parseExcel($filePath)
{
    $data = [];
    // Use PhpSpreadsheet library
    $spreadsheet = \PhpOffice\PhpSpreadsheet\IOFactory::load($filePath);
    $worksheet = $spreadsheet->getActiveSheet();

    $rows = [];
    foreach ($worksheet->getRowIterator() as $row) {
        $cellIterator = $row->getCellIterator();
        $cellIterator->setIterateOnlyExistingCells(false);
        $cells = [];
        foreach ($cellIterator as $cell) {
            $cells[] = $cell->getValue();
        }
        $rows[] = $cells;
    }

    $header = null;
    foreach ($rows as $row) {
        if (!$header) {
            $header = $row;
        } else {
            if (count($header) != count($row)) {
                // Skip or handle error
                continue;
            }
            $data[] = array_combine($header, $row);
        }
    }

    return $data;
}

// Function to determine data types and build Elasticsearch mapping
function buildMapping($data)
{
    $mapping = [
        'mappings' => [
            'properties' => []
        ]
    ];

    $fieldTypes = [];

    // Analyze data types for each field
    $numSamples = min(10, count($data));

    foreach ($data[0] as $field => $value) {
        $fieldValues = [];

        for ($i = 0; $i < $numSamples; $i++) {
            $fieldValues[] = $data[$i][$field];
        }

        $fieldType = determineFieldType($fieldValues);
        $mapping['mappings']['properties'][$field] = ['type' => $fieldType];
    }

    return $mapping;
}

// Function to determine field type based on sample values
function determineFieldType($values)
{
    $types = ['integer', 'float', 'date', 'text'];

    foreach ($types as $type) {
        $allMatch = true;
        foreach ($values as $value) {
            if (!isOfType($value, $type)) {
                $allMatch = false;
                break;
            }
        }
        if ($allMatch) {
            return $type;
        }
    }
    // Default to text if no type matches all values
    return 'text';
}

// Function to check if a value is of a certain type
function isOfType($value, $type)
{
    switch ($type) {
        case 'integer':
            return filter_var($value, FILTER_VALIDATE_INT) !== false;
        case 'float':
            return filter_var($value, FILTER_VALIDATE_FLOAT) !== false;
        case 'date':
            return strtotime($value) !== false;
        case 'text':
            return is_string($value);
        default:
            return false;
    }
}

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

// Function to create the index with the generated mapping
function createIndex($esClient, $indexName, $mapping)
{
    $params = [
        'index' => $indexName,
        'body' => $mapping,
    ];

    try {
        $esClient->indices()->create($params);
        echo "Index '$indexName' created successfully with proper mappings.\n";
    } catch (Exception $e) {
        echo 'Error creating index: ' . $e->getMessage() . "\n";
    }
}

// Function to index data into Elasticsearch using bulk API
function indexData($esClient, $indexName, $data)
{
    $params = ['body' => []];
    $i = 0;
    foreach ($data as $row) {
        $params['body'][] = [
            'index' => [
                '_index' => $indexName,
            ]
        ];
        $params['body'][] = $row;

        // Every 1000 documents, send a bulk request
        if ($i % 1000 == 0 && $i != 0) {
            try {
                $responses = $esClient->bulk($params);
            } catch (Exception $e) {
                echo 'Error indexing data: ' . $e->getMessage() . "\n";
            }
            // Erase the old bulk request
            $params = ['body' => []];
        }
        $i++;
    }

    // Send any remaining data
    if (!empty($params['body'])) {
        try {
            $responses = $esClient->bulk($params);
        } catch (Exception $e) {
            echo 'Error indexing data: ' . $e->getMessage() . "\n";
        }
    }
}
