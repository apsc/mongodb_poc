const { MongoClient } = require('mongodb');

async function getOneRestaurantDocument() {
  const client = new MongoClient('mongodb+srv://amanpsc2:Y1EoXsq9QGAVdmWc@imt576.0z0jy.mongodb.net/');

  try {
    // Connect to MongoDB Atlas
    await client.connect();
    console.log("Connected to MongoDB Atlas");

    // Select the database and collection
    const db = client.db('IMT576');
    const restaurantCollection = db.collection('restaurant');

    // Getting number of documents
    const totalDocuments = await restaurantCollection.countDocuments();
    console.log("Total Documents: " + totalDocuments);

    // Find restaurants in Brooklyn
    const brooklynRestaurants = await restaurantCollection.find({ 
        "borough": "Missing" 
    }).toArray();
      
    console.log(`Found ${brooklynRestaurants.length} restaurants in Brooklyn`);

    // Find restaurants in Brooklyn and cusine is Bakery
    const brooklynBakeryRestaurants = await restaurantCollection.find({ 
        "borough": "Brooklyn",
        "cuisine": "Bakery" 
    }).toArray();
      
    console.log(`Found ${brooklynBakeryRestaurants.length} restaurants in Brooklyn with cuisine Bakery`);


    // Interesting query that provides insights into data
    // (limit the result to 10 groups)
    const insightsPipeline = [
        // Flatten the grades array so each grade becomes a separate document
        { $unwind: "$grades" },
        // Group by borough and cuisine and calculate the average score and total count
        {
          $group: {
            _id: { borough: "$borough", cuisine: "$cuisine" },
            averageScore: { $avg: "$grades.score" },
            count: { $sum: 1 }
          }
        },
        // Sort the groups by average Score (ascending order: lower scores are better)
        { $sort: { averageScore: 1 } },
        // Limit the pipeline output to 10 groups
        { $limit: 10 },
        // Reshape the output
        {
          $project: {
            _id: 0,
            borough: "$_id.borough",
            cuisine: "$_id.cuisine",
            averageScore: { $round: ["$averageScore", 2] },
            count: 1
          }
        }
    ];
    
    const insights = await restaurantCollection.aggregate(insightsPipeline).toArray();
    console.log("Aggregation Results:");
    insights.forEach(result => {
    console.log(`Borough: ${result.borough}, Cuisine: ${result.cuisine}, Average Score: ${result.averageScore}, Count: ${result.count}`);
    });

      
    console.log(`Found ${brooklynBakeryRestaurants.length} restaurants in Brooklyn with cuisine Bakery`);
    
    // Get just one document from the collection
    const document = await restaurantCollection.find().limit(1).toArray();
    
    // Print the document with nice formatting
    // console.log("Retrieved document:");
    // console.log(JSON.stringify(document[0], null, 2));
    
    // Alternative method using findOne()
    // const document = await restaurantCollection.findOne();
    // console.log(JSON.stringify(document, null, 2));
    
    return document;
  } catch (error) {
    console.error('Error retrieving data from MongoDB:', error);
  } finally {
    // Close the connection
    await client.close();
    console.log("MongoDB connection closed");
  }
}

// Call the function
getOneRestaurantDocument();
