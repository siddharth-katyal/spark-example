async function insertRecords(numRecords) {
    var docs = [];
    for (var i = 0; i < numRecords; i++) {
        var doc = {
            "id":  Math.floor(Math.random() * (100000000000 - 0) + 0),
            "name": Math.random().toString(36).substring(2, 15),
            "state_id":  Math.floor(Math.random() * (100000000000 - 0) + 0),
            "state_code": Math.random().toString(36).substring(2, 4),
            "country_id":  Math.floor(Math.random() * (100000000000 - 0) + 0),
            "country_code": Math.random().toString(36).substring(2, 4),
            "latitude": Math.random().toString(36).substring(2, 10),
            "longitude": Math.random().toString(36).substring(2, 10)
        }      
        };
        {

        docs.push(doc);
    }
       return await db.getCollection("test").insertMany(docs);
}