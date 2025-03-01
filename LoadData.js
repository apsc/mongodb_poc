const fs = require('fs');
const { MongoClient } = require('mongodb');
const readline = require('readline');

async function importData() {
  // Create a new MongoClient (removed deprecated option)
  const client = new MongoClient('mongodb+srv://amanpsc2:Y1EoXsq9QGAVdmWc@imt576.0z0jy.mongodb.net/');

  try {
    // Connect to the MongoDB server
    await client.connect();
    console.log("Connected to MongoDB Atlas");

    // Select the database 'IMT576'
    const db = client.db('IMT576');

    // Select the collection
    const collection = db.collection('restaurant');

    // Create a readable stream from the file
    const fileStream = fs.createReadStream('data/primer-dataset.json');
    
    // Process the file line by line
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });

    let documents = [];
    let lineCount = 0;
    
    // Process each line
    for await (const line of rl) {
      if (line.trim()) {  // Skip empty lines
        try {
          const doc = JSON.parse(line);
          documents.push(doc);
          lineCount++;
          
          // Insert in batches of 1000 to avoid memory issues
          if (documents.length >= 1000) {
            await collection.insertMany(documents);
            console.log(`Inserted batch of ${documents.length} documents`);
            documents = [];
          }
        } catch (error) {
          console.error(`Error parsing JSON at line ${lineCount + 1}:`, error);
          console.error(`Problematic line: ${line.substring(0, 100)}...`);
        }
      }
    }
    
    // Insert any remaining documents
    if (documents.length > 0) {
      await collection.insertMany(documents);
      console.log(`Inserted final batch of ${documents.length} documents`);
    }
    
    console.log(`Total lines processed: ${lineCount}`);
    
  } catch (error) {
    console.error('Error connecting to MongoDB or inserting data:', error);
  } finally {
    // Close the connection
    await client.close();
    console.log("MongoDB connection closed");
  }
}

// importData();




async function testDataLoad() {
  const client = new MongoClient('mongodb+srv://amanpsc2:@imt576.0z0jy.mongodb.net/');

  try {
    // Connect to the MongoDB server
    await client.connect();
    console.log("Connected to MongoDB Atlas");

    // Select the database 'IMT576'
    const db = client.db('IMT576');
    
    // Insert a few documents into the sales collection.
    const salesCollection = db.collection('sales');
    await salesCollection.insertMany([
      { 'item': 'abc', 'price': 10, 'quantity': 2, 'date': new Date('2014-03-01T08:00:00Z') },
      { 'item': 'jkl', 'price': 20, 'quantity': 1, 'date': new Date('2014-03-01T09:00:00Z') },
      { 'item': 'xyz', 'price': 5, 'quantity': 10, 'date': new Date('2014-03-15T09:00:00Z') },
      { 'item': 'xyz', 'price': 5, 'quantity': 20, 'date': new Date('2014-04-04T11:21:39.736Z') },
      { 'item': 'abc', 'price': 10, 'quantity': 10, 'date': new Date('2014-04-04T21:23:13.331Z') },
      { 'item': 'def', 'price': 7.5, 'quantity': 5, 'date': new Date('2015-06-04T05:08:13Z') },
      { 'item': 'def', 'price': 7.5, 'quantity': 10, 'date': new Date('2015-09-10T08:43:00Z') },
      { 'item': 'abc', 'price': 10, 'quantity': 5, 'date': new Date('2016-02-06T20:20:13Z') },
    ]);
    
    // Run a find command to view items sold on April 4th, 2014.
    const salesOnApril4th = await salesCollection.find({
      date: { $gte: new Date('2014-04-04'), $lt: new Date('2014-04-05') }
    }).count();
    
    // Print a message to the output window.
    console.log(`${salesOnApril4th} sales occurred in 2014.`);
    
    // Here we run an aggregation and open a cursor to the results.
    const aggregationResults = await salesCollection.aggregate([
      // Find all of the sales that occurred in 2014.
      { $match: { date: { $gte: new Date('2014-01-01'), $lt: new Date('2015-01-01') } } },
      // Group the total sales for each product.
      { $group: { _id: '$item', totalSaleAmount: { $sum: { $multiply: [ '$price', '$quantity' ] } } } }
    ]).toArray();
    
    console.log("Aggregation Results:", aggregationResults);
    
  } catch (error) {
    console.error('Error connecting to MongoDB or inserting data:', error);
  } finally {
    // Close the connection
    await client.close();
    console.log("MongoDB connection closed");
  }
}

testDataLoad();
