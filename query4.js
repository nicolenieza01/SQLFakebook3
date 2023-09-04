// Query 4
// Find user pairs (A,B) that meet the following constraints:
// i) user A is male and user B is female
// ii) their Year_Of_Birth difference is less than year_diff
// iii) user A and B are not friends
// iv) user A and B are from the same hometown city
// The following is the schema for output pairs:
// [
//      [user_id1, user_id2],
//      [user_id1, user_id3],
//      [user_id4, user_id2],
//      ...
//  ]
// user_id is the field from the users collection. Do not use the _id field in users.
// Return an array of arrays.

function suggest_friends(year_diff, dbname) {
    db = db.getSiblingDB(dbname);

    let pairs = [];
    db.users.find({gender: "male"}).forEach(function(userA) {
      // male and female nested loop check 
      db.users.find({gender: "female", hometown: {city: userA.hometown.city}}).forEach(function(userB) {
        // check if already friends using the index function
        if (userA.friends.indexOf(userB._id) === -1) {

          // check year of births are good 
          if (Math.abs(userA.YOB - userB.YOB) >= year_diff) {
            // do not PUSH into pairs 
        }
        else{
          //otherwise ur good 
          pairs.push([userA._id, userB._id]);
        }
      }
    });
  });
          
    // TODO: implement suggest friends


    return pairs;
}
