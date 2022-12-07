import fs from 'fs';
import util from 'util';
import mysql from 'mysql';
import Queue from 'bee-queue';
import dotenv from 'dotenv';
import chalk from 'chalk';

dotenv.config();

import { fetch } from './requestUtils.mjs';
import { values } from './dataUtils.mjs';

console.log(chalk.hex('#e3315b')('VOLUSPA'));

const concurrencyLimit = 500;

// connect queue
const bungieQueue = new Queue('bungie', {
  // storeJobs: false,
});

// make sure there's no jobs left over from earlier
await bungieQueue.destroy();

// setup basic db stuff
const pool = mysql.createPool({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  connectionLimit: 100,
  supportBigNumbers: true,
  multipleStatements: true,
  charset: 'utf8mb4',
});

// make db query async
const query = util.promisify(pool.query).bind(pool);

// get a list of members to fetch profile data for
const members = await query('SELECT id, membershipType, membershipId FROM members WHERE NOT isPrivate');

// bee-queue jobs
for (let i = 0; i < members.length + 100000; i += 100000) {
  console.log('Adding to queue: %s', i);

  await bungieQueue
    .saveAll(
      members
        .slice(i, i + 100000)
        .map((member) =>
          bungieQueue
            .createJob({
              membershipType: member.membershipType,
              membershipId: member.membershipId,
            })
            .retries(3)
        )
    )
    .then((errors) => {
      if (errors.size > 0) {
        console.log(errors);
      }
    });
}

// empty objects to hold statistics for later
const StatsTriumphs = {};
const StatsCollections = {};
const StatsParallelProgram = [];

// variables for relaying progress to the console
let jobCompletionValue = members.length;
let jobProgress = 0;
let jobSuccessful = 0;

bungieQueue.on('job succeeded', (id, result) => {
  jobProgress++;
  jobSuccessful++;

  // console.log(result);
});

bungieQueue.on('job retrying', (id, error) => {
  console.log(error.message.indexOf('Bungie error') > -1 || error.message.indexOf('HTTP failure') > -1 ? chalk.hex('#f44336')(`${error.message} (will retry)`) : chalk.dim(`${error.message} (will retry)`));
});

bungieQueue.on('job failed', (id, error) => {
  console.log(error.message.indexOf('Bungie error') > -1 || error.message.indexOf('HTTP failure') > -1 ? chalk.hex('#f44336')(error.message) : chalk.dim(error.message));

  jobProgress++;
});

const scrapeStart = new Date();

// ignition
bungieQueue.process(concurrencyLimit, processJob);

async function processJob(job) {
  try {
    /**
     * 1. when only fetch runs, requests complete in ~15 seconds
     */

    // const fetchStart = performance.now();
    const response = await fetch(`https://www.bungie.net/Platform/Destiny2/${job.data.membershipType}/Profile/${job.data.membershipId}/?components=100,800,900`);
    // const fetchEnd = performance.now();

    // await fs.promises.writeFile(`./cache/${job.data.membershipId}.json`, JSON.stringify(response))

    // return `${job.id}: fetch ${fetchEnd - fetchStart}ms`;

    /**
     * 2. when the response returned by the fetch function is accessed by the below code,
     *    code which completes consistently in less than 15ms,
     *    the fetch function instead takes ~45 seconds to complete.
     * 
     *    the first 30 runs complete in a reasonable time, but quickly balloon out.
     * 
     *    current theory: accessing the response causes it to persist and shit
     */

    // const jobStart = performance.now();

    processResponse(job, response)

    // const jobEnd = performance.now();

    // return `${job.id}: fetch ${fetchEnd - fetchStart}ms, process ${jobEnd - jobStart}ms`;

    return true;
  } catch (error) {
    throw new Error(error.message);
  }
}

function processResponse(job, response) {
  if (response && response.ErrorCode !== undefined) {
    if (response.ErrorCode === 1) {
      if (response.Response.profileRecords.data === undefined || Object.keys(response.Response.characterCollectibles.data).length === 0) {
        let displayName = '';

        try {
          displayName = response.Response.profile.data.userInfo.displayName;
        } catch (e) {}

        query(mysql.format(`UPDATE members SET isPrivate = '1' WHERE membershipId = ?`, [job.data.membershipId]));

        job.retries(0);

        throw new Error(`${job.data.membershipId} (${job.id}): ${displayName}'s profile is private`);
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
      if (collections.indexOf('3316003520') > -1) {
        StatsParallelProgram.push({
          membershipType: job.data.membershipType,
          membershipId: job.data.membershipId,
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
        query(
          mysql.format(
            `INSERT INTO voluspa.profiles (
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
              job.data.membershipType,
              job.data.membershipId,
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
    } else if (response.ErrorCode === 0) {
      throw new Error(`${job.data.membershipId} (${job.id}): HTTP failure`);
    } else {
      throw new Error(`${job.data.membershipId} (${job.id}): Bungie error ${response.ErrorCode}`);
    }
  } else {
    console.log('fuck');
  }
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
      await query(mysql.format(`INSERT INTO voluspa.scrapes_status (scrape, duration, members) VALUES (?, ?, ?)`, [scrapeStart, Math.ceil((Date.now() - scrapeStart.getTime()) / 60000), jobSuccessful]));

      const ranks = await query(`SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    INSERT INTO voluspa.ranks (
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
            FROM voluspa.profiles
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
    COMMIT;`);

      console.log(ranks);

      await query(mysql.format(`UPDATE voluspa.config SET lastScrapeStart = ?, lastScrapeDuration = ?, lastRanked = ?, membersAggregate = ?, membersScrape = ? WHERE config.id = 1`, [scrapeStart, Math.ceil(Date.now() - scrapeStart.getTime()), new Date(), jobCompletionValue, jobSuccessful]));

      await fetch('https://b.vlsp.network/Generate');

      await query(mysql.format(`INSERT INTO voluspa.statistics (scrape, hash, commonality) VALUES ?`, [Object.entries(StatsTriumphs).map(([hash, commonality]) => [scrapeStart, hash, commonality])]));
      console.log('Saved Triumphs stats to database');

      await query(mysql.format(`INSERT INTO voluspa.statistics (scrape, hash, commonality) VALUES ?`, [Object.entries(StatsCollections).map(([hash, commonality]) => [scrapeStart, hash, commonality])]));
      console.log('Saved Collections stats to database');
    }

    process.exit();
  } else {
    await Promise.all([
      fs.promises.writeFile('./temp/triumphs.temp.json', JSON.stringify(StatsTriumphs)), //
      fs.promises.writeFile('./temp/collections.temp.json', JSON.stringify(StatsCollections)),
      fs.promises.writeFile('./temp/parallel-program.temp.json', JSON.stringify(StatsParallelProgram)),
    ]);
  }

  console.log(`${jobProgress}/${jobCompletionValue} // ${((jobProgress / jobCompletionValue) * 100).toFixed(3)}% // ${Math.ceil((Date.now() - scrapeStart.getTime()) / 60000)}m elapsed, ~${Math.floor((((Date.now() - scrapeStart.getTime()) / jobProgress) * (jobCompletionValue - jobProgress)) / 60000)}m remaining // Parallel Programs: ${StatsParallelProgram.length}`);
}

const updateIntervalTimer = setInterval(updateLog, 10000);
