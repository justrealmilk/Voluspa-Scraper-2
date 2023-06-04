import fs from 'fs';
import mysql from 'mysql2';
import pLimit from 'p-limit';
import dotenv from 'dotenv';
import chalk from 'chalk';

import { customFetch } from './requestUtils.js';
import { values } from './dataUtils.js';

dotenv.config();

console.log(chalk.hex('#e3315b')('VOLUSPA'));

const limit = pLimit(200);

// setup basic db stuff
const puddle = mysql.createPool({
  host: process.env.MYSQL_HOST,
  port: process.env.MYSQL_PORT,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  connectionLimit: 20,
  supportBigNumbers: true,
  multipleStatements: true,
  charset: 'utf8mb4',
});

// make db query async
const pool = puddle.promise();

// get a list of members to fetch profile data for
console.log('Querying braytech.members');
const [members] = await pool.query('SELECT id, membershipType, membershipId FROM braytech.members WHERE NOT isPrivate ORDER BY last DESC LIMIT 0, 1000000');
console.log('Results received');

// empty objects to hold statistics for later
const StatsTriumphs = {};
const StatsCollections = {};
const StatsParallelProgram = [];

// variables for relaying progress to the console
let jobCompletionValue = members.length;
let jobProgress = 0;
let jobSuccessful = 0;

const jobs = members.map((member) => limit(() => processJob(member)));

const scrapeStart = new Date();

async function processJob(job) {
  try {
    const processStart = new Date().toISOString();

    const fetchStart = performance.now();
    const response = await customFetch(`https://www.bungie.net/Platform/Destiny2/${job.membershipType}/Profile/${job.membershipId}/?components=100,800,900`);
    const fetchEnd = performance.now();

    const computeStart = performance.now();
    const result = await processResponse(job, response);
    const computeEnd = performance.now();

    jobProgress++;
    jobSuccessful++;

    return {
      error: result,
      membership: {
        membershipType: job.membershipType,
        membershipId: job.membershipId,
      },
      performance: {
        start: processStart,
        fetch: fetchEnd - fetchStart,
        compute: computeEnd - computeStart,
      },
    };
  } catch (error) {
    jobProgress++;

    return {
      error: error.message,
      performance: undefined,
    };
  }
}

function processResponse(job, response) {
  if (response && response.ErrorCode !== undefined) {
    if (response.ErrorCode === 1) {
      if (response.Response.profileRecords.data === undefined || Object.keys(response.Response.characterCollectibles.data).length === 0) {
        try {
          displayName = response.Response.profile.data.userInfo.displayName;
        } catch (e) {}

        if (process.env.STORE_JOB_RESULTS === 'true') {
          pool.query(mysql.format(`UPDATE braytech.members SET isPrivate = '1' WHERE membershipId = ?`, [job.membershipId]));
        }

        return 'PRIVATE_PROFILE';
      }

      const triumphs = [];
      const collections = [];

      // triumphs (profile scope)
      for (const hash in response.Response.profileRecords.data.records) {
        const record = response.Response.profileRecords.data.records[hash];

        if (record.intervalObjectives && record.intervalObjectives.length) {
          if (record.intervalObjectives.some((objective) => objective.complete) && record.intervalObjectives.filter((objective) => objective.complete).length === record.intervalObjectives.length) {
            triumphs.push(hash);
          }
        } else {
          if (!!(record.state & 1) || !!!(record.state & 4)) {
            triumphs.push(hash);
          }
        }
      }

      // triumphs (character scope)
      for (const characterId in response.Response.characterRecords.data) {
        for (const hash in response.Response.characterRecords.data[characterId].records) {
          const record = response.Response.characterRecords.data[characterId].records[hash];

          if (triumphs.indexOf(hash) === -1) {
            if (record.intervalObjectives && record.intervalObjectives.length) {
              if (record.intervalObjectives.some((objective) => objective.complete) && record.intervalObjectives.filter((objective) => objective.complete).length === record.intervalObjectives.length) {
                triumphs.push(hash);
              }
            } else {
              if (!!(record.state & 1) || !!!(record.state & 4)) {
                triumphs.push(hash);
              }
            }
          }
        }
      }

      // collections (profile scope)
      for (const hash in response.Response.profileCollectibles.data.collectibles) {
        if (!!!(response.Response.profileCollectibles.data.collectibles[hash].state & 1)) {
          collections.push(hash);
        }
      }

      // collections (character scope)
      for (const characterId in response.Response.characterCollectibles.data) {
        for (const hash in response.Response.characterCollectibles.data[characterId].collectibles) {
          if (!!!(response.Response.characterCollectibles.data[characterId].collectibles[hash].state & 1)) {
            if (collections.indexOf(hash) === -1) {
              collections.push(hash);
            }
          }
        }
      }

      // for spying 🥸
      if (collections.includes('3316003520')) {
        StatsParallelProgram.push({
          membershipType: job.membershipType,
          membershipId: job.membershipId,
        });
      }

      for (let index = 0; index < triumphs.length; index++) {
        const hash = triumphs[index];

        if (StatsTriumphs[hash]) {
          StatsTriumphs[hash]++;
        } else {
          StatsTriumphs[hash] = 1;
        }
      }

      for (let index = 0; index < collections.length; index++) {
        const hash = collections[index];

        if (StatsCollections[hash]) {
          StatsCollections[hash]++;
        } else {
          StatsCollections[hash] = 1;
        }
      }

      // values specifically for storing in the database for things like leaderboards
      const PreparedValues = values(response);

      const date = new Date();

      if (process.env.STORE_JOB_RESULTS === 'true') {
        pool.query(
          mysql.format(
            `INSERT INTO profiles.members (
                membershipType,
                membershipId,
                displayName,
                lastUpdated,
                lastPlayed,
                triumphScore,
                legacyScore,
                activeScore,
                collectionsTotal
              )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?
              )
            ON DUPLICATE KEY UPDATE displayName = ?, lastUpdated = ?, lastPlayed = ?, triumphScore = ?, legacyScore = ?, activeScore = ?, collectionsTotal = ?`,
            [
              job.membershipType,
              job.membershipId,
              PreparedValues.displayName,
              date, //
              PreparedValues.lastPlayed,
              PreparedValues.triumphScore,
              PreparedValues.legacyScore,
              PreparedValues.activeScore,
              collections.length,
              // update...
              PreparedValues.displayName,
              date, //
              PreparedValues.lastPlayed,
              PreparedValues.triumphScore,
              PreparedValues.legacyScore,
              PreparedValues.activeScore,
              collections.length,
            ]
          )
        );
      }

      return 'SUCCESS';
    } else if (response.ErrorCode !== undefined) {
      return 'BUNGIE_ERROR';
    }
  }

  return 'UNKNOWN_ERROR';
}

// just in case
let finalising = false;

// relaying updates to the console/saving final statistics
async function updateLog() {
  if (jobProgress === jobCompletionValue && finalising === false) {
    finalising = true;
    clearInterval(updateIntervalTimer);

    await fs.promises.copyFile('./temp/triumphs.json', './temp/triumphs.previous.json');
    await fs.promises.writeFile('./temp/triumphs.json', JSON.stringify(StatsTriumphs));
    console.log('Saved Triumphs stats to disk');

    await fs.promises.copyFile('./temp/collections.json', './temp/collections.previous.json');
    await fs.promises.writeFile('./temp/collections.json', JSON.stringify(StatsCollections));
    console.log('Saved Collections stats to disk');

    await fs.promises.copyFile('./temp/parallel-program.json', './temp/parallel-program.previous.json');
    await fs.promises.writeFile('./temp/parallel-program.json', JSON.stringify(StatsParallelProgram));
    console.log('Saved Parallel Program stats to disk');

    if (process.env.STORE_JOB_RESULTS === 'true') {
      const scrapesStatusQuery = mysql.format(`INSERT INTO profiles.scrapes (date, duration, crawled, assessed) VALUES (?, ?, ?, ?);`, [scrapeStart, Math.ceil((Date.now() - scrapeStart.getTime()) / 60000), jobCompletionValue, jobSuccessful]);
      const rankQuery = `SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
      INSERT INTO leaderboards.ranks (
          membershipType,
          membershipId,
          triumphScore,
          legacyScore,
          activeScore,
          collectionsTotal,
          triumphRank,
          legacyRank,
          activeRank,
          collectionsRank
        ) (
          SELECT membershipType,
            membershipId,
            triumphScore,
            legacyScore,
            activeScore,
            collectionsTotal,
            triumphRank,
            legacyRank,
            activeRank,
            collectionsRank
          FROM (
              SELECT *,
                DENSE_RANK() OVER (
                  ORDER BY triumphScore DESC
                ) triumphRank,
                DENSE_RANK() OVER (
                  ORDER BY legacyScore DESC
                ) legacyRank,
                DENSE_RANK() OVER (
                  ORDER BY activeScore DESC
                ) activeRank,
                DENSE_RANK() OVER (
                  ORDER BY collectionsTotal DESC
                ) collectionsRank
              FROM profiles.members
              WHERE lastPlayed > '2022-02-22 17:00:00' 
              ORDER BY displayName ASC
            ) R
        ) ON DUPLICATE KEY
      UPDATE triumphScore = R.triumphScore,
        legacyScore = R.legacyScore,
        activeScore = R.activeScore,
        collectionsTotal = R.collectionsTotal,
        triumphRank = R.triumphRank,
        legacyRank = R.legacyRank,
        activeRank = R.activeRank,
        collectionsRank = R.collectionsRank;
      COMMIT;`;

      const statsTriumphsQuery = mysql.format(`INSERT INTO profiles.commonality (date, hash, value) VALUES ?;`, [Object.entries(StatsTriumphs).map(([hash, value]) => [scrapeStart, hash, value])]);
      const statsCollectiblesQuery = mysql.format(`INSERT INTO profiles.commonality (date, hash, value) VALUES ?;`, [Object.entries(StatsCollections).map(([hash, value]) => [scrapeStart, hash, value])]);

      await fs.promises.writeFile(`./temp/queries.${Date.now()}.sql`, `${scrapesStatusQuery}\n\n${rankQuery}`);
      await fs.promises.writeFile(`./temp/queries.extended.${Date.now()}.sql`, `${statsTriumphsQuery}\n\n${statsCollectiblesQuery}`);

      const [status] = await pool.query(scrapesStatusQuery);

      const [ranks] = await pool.query(rankQuery);
      console.log(ranks);

      await pool.query(statsTriumphsQuery);
      console.log('Saved Triumphs stats to database');

      await pool.query(statsCollectiblesQuery);
      console.log('Saved Collections stats to database');

      await fetch(`http://0.0.0.0:8080/Generate/Commonality?id=${status.insertId}`, {
        headers: {
          'x-api-key': 'insomnia',
        },
      });
      await fetch('http://0.0.0.0:8080/Generate', {
        headers: {
          'x-api-key': 'insomnia',
        },
      });
    }

    process.exit();
  } else {
    await Promise.all([
      fs.promises.writeFile('./temp/triumphs.temp.json', JSON.stringify(StatsTriumphs)), //
      fs.promises.writeFile('./temp/collections.temp.json', JSON.stringify(StatsCollections)),
      fs.promises.writeFile('./temp/parallel-program.temp.json', JSON.stringify(StatsParallelProgram)),
    ]);
  }

  console.table({
    Progress: Math.floor((jobProgress / jobCompletionValue) * 100),
    JobProgress: jobProgress,
    JobCompletionValue: jobCompletionValue,
    QueueActive: limit.activeCount,
    QueuePending: limit.pendingCount,
    TimeElapsed: Math.ceil((Date.now() - scrapeStart.getTime()) / 60000),
    TimeRemaining: Math.floor((((Date.now() - scrapeStart.getTime()) / Math.max(jobProgress, 1)) * (jobCompletionValue - jobProgress)) / 60000),
    TimeComplete: new Date(Date.now() + ((Date.now() - scrapeStart.getTime()) / Math.max(jobProgress, 1)) * (jobCompletionValue - jobProgress)).toLocaleString('en-AU', { dateStyle: 'full', timeStyle: 'long', hour12: false, timeZone: 'Australia/Brisbane' }),
    ParallelPrograms: StatsParallelProgram.length,
  });
}

const updateIntervalTimer = setInterval(updateLog, 10000);

const jobResults = await Promise.all(jobs);

await fs.promises.copyFile('./temp/job-results.json', './temp/job-results.previous.json');
await fs.promises.writeFile('./temp/job-results.json', JSON.stringify(jobResults));
console.log('Saved job results to disk');
