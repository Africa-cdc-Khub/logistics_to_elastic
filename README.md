# MySQL to Elasticsearch Data Loader

This project is a Node.js application that loads data from a MySQL table or view into an Elasticsearch index. The data is related to logistics and financial information from Africa CDC events, and it is automatically updated every three hours using a cron job.

## Features
- Connects to a MySQL database to retrieve data from a table or view.
- Sends data to an Elasticsearch index.
- Automatically updates data every 3 hours using cron jobs.
- Uses environment variables stored in a `.env` file for configuration.

## Prerequisites

Before running the application, ensure that you have the following installed:
- [Node.js](https://nodejs.org/en/) (version 12 or above)
- [MySQL](https://www.mysql.com/) server
- [Elasticsearch](https://www.elastic.co/elasticsearch/) server

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/your-username/your-repo.git
cd your-repo
