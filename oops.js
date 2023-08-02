import fs from 'fs';
import mysql from 'mysql2';
import dotenv from 'dotenv';

dotenv.config();

console.log('VOLUSPA');

// setup basic db stuff
const puddle = mysql.createPool({
  host: process.env.MYSQL_HOST,
  port: process.env.MYSQL_PORT,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  connectionLimit: 10,
  acquireTimeout: 60000,
  supportBigNumbers: true,
  multipleStatements: true,
  charset: 'utf8mb4',
});

// make db query async
const pool = puddle.promise();

// const rankQuery = mysql.format(
//   `SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
// TRUNCATE leaderboards.ranks;
// INSERT INTO leaderboards.ranks (
//     membershipType,
//     membershipId,
//     displayName,
//     triumphScore,
//     legacyScore,
//     activeScore,
//     collectionsScore,
//     triumphRank,
//     legacyRank,
//     activeRank,
//     collectionsRank
//   ) (
//     SELECT membershipType,
//       membershipId,
//       displayName,
//       triumphScore,
//       legacyScore,
//       activeScore,
//       collectionsScore,
//       triumphRank,
//       legacyRank,
//       activeRank,
//       collectionsRank
//     FROM (
//         SELECT *,
//           DENSE_RANK() OVER (
//             ORDER BY triumphScore DESC
//           ) triumphRank,
//           DENSE_RANK() OVER (
//             ORDER BY legacyScore DESC
//           ) legacyRank,
//           DENSE_RANK() OVER (
//             ORDER BY activeScore DESC
//           ) activeRank,
//           DENSE_RANK() OVER (
//             ORDER BY collectionsScore DESC
//           ) collectionsRank
//         FROM profiles.members
//         WHERE lastUpdated >= ? AND lastPlayed > '2023-02-28 17:00:00' 
//         ORDER BY displayName ASC
//       ) R
//   ) ON DUPLICATE KEY
// UPDATE displayName = R.displayName,
//   triumphScore = R.triumphScore,
//   legacyScore = R.legacyScore,
//   activeScore = R.activeScore,
//   collectionsScore = R.collectionsScore,
//   triumphRank = R.triumphRank,
//   legacyRank = R.legacyRank,
//   activeRank = R.activeRank,
//   collectionsRank = R.collectionsRank;
// COMMIT;`,
//   ['2023-07-11 12:37:22']
// );

// console.log('Evaluate leaderboards...');
// const ranks = await pool.query(rankQuery);
// console.log(ranks);
// console.log('Evaluated leaderboards...');

// const statsTriumphsQuery = mysql.format(`INSERT INTO profiles.commonality (date, hash, value) VALUES ?;`, [Object.entries(await fs.promises.readFile(`./triumphs.json`, 'utf8').then((string) => JSON.parse(string))).map(([hash, value]) => ['2023-07-11 12:37:22', hash, value])]);
// const statsCollectiblesQuery = mysql.format(`INSERT INTO profiles.commonality (date, hash, value) VALUES ?;`, [Object.entries(await fs.promises.readFile(`./collections.json`, 'utf8').then((string) => JSON.parse(string))).map(([hash, value]) => ['2023-07-11 12:37:22', hash, value])]);

// console.log('Save Triumphs stats to database...');
// await pool.query(statsTriumphsQuery);
// console.log('Saved Triumphs stats to database...');

// console.log('Save Collections stats to database...');
// await pool.query(statsCollectiblesQuery);
// console.log('Saved Collections stats to database...');

// console.log('Cache commonality...');
// await fetch(`http://0.0.0.0:8080/Generate/Commonality?id=1834`, {
//   headers: {
//     'x-api-key': 'insomnia',
//   },
// });
// console.log('Cached commonality...');
// console.log('Generate common...');
// await fetch('http://0.0.0.0:8080/Generate', {
//   headers: {
//     'x-api-key': 'insomnia',
//   },
// });
// console.log('Generated common...');
    
