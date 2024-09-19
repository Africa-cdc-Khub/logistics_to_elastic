require('dotenv').config(); // Load environment variables from .env
const mysql = require('mysql');
const { Client } = require('@elastic/elasticsearch');
const cron = require('cron');

// Set up MySQL connection using environment variables
const db = mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
});

db.connect(err => {
    if (err) {
        console.error('Error connecting to MySQL: ', err);
        return;
    }
    console.log('Connected to MySQL');
});

// Set up Elasticsearch client using environment variables
const esClient = new Client({ node: process.env.ES_HOST });

// Function to load data into Elasticsearch
const loadDataToElasticsearch = () => {
    const query = 'SELECT * FROM combined_table_view';  // Use your actual table/view name

    db.query(query, (err, results) => {
        if (err) throw err;

        // Loop through the results and send each row to Elasticsearch
        results.forEach(async (row) => {
            try {
                await esClient.index({
                    index: 'africa_cdc_logistics',
                    body: {
                        event_name: row['event'],  // Event name
                        member_state: row['member_state'],  // Country or organization involved
                        item: {
                            name: row['item_name'],  // Name of the item
                            category: row['category_name'],  // Category of the item
                            quantity: row['qty'],  // Quantity of items
                            unit_price: row['unit_price'],  // Price per unit
                            total_cost: row['total_cost'],  // Total cost
                        },
                        logistics: {
                            weight_kg: row['total_weight_kg'],  // Total weight in kg
                            weight_ton: row['total_weight_ton'],  // Total weight in tons
                            volume_cbm: row['volume_cbm'],  // Volume in cubic meters
                            delivery_date: row['delivery_date'],  // Date of delivery
                        }
                    },
                });
                console.log('Data indexed successfully');
            } catch (e) {
                console.error('Error indexing data: ', e);
            }
        });
    });
};

// Schedule the task to run every 3 hours
const job = new cron.CronJob('0 */3 * * *', loadDataToElasticsearch, null, true, 'America/Los_Angeles');
job.start();
