import fs from 'fs';
import mysql from 'mysql2';
import pQueue from 'p-queue';
import dotenv from 'dotenv';
import http from 'http';

import { customFetch } from './requestUtils.js';
import { values } from './dataUtils.js';

dotenv.config();

console.log('VOLUSPA');

let metrics = '';

const requestListener = function (request, response) {
  response.writeHead(200);
  response.end(metrics);
};

const server = http.createServer(requestListener);

server.listen(8181, '0.0.0.0', () => {
  console.log(`HTTP server started`);
});

const queue = new pQueue({ concurrency: 150 });

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

// get a list of members to fetch profile data for
console.log('Querying braytech.members');
const [members] = await pool.query('SELECT id, membershipType, membershipId FROM braytech.members WHERE NOT isPrivate LIMIT 0, 1000000');
console.log('Results received');

// empty objects to hold statistics for later
const StatsTriumphs = {};
const StatsCollections = {};
const StatsParallelProgram = [];

// variables for relaying progress to the console
let jobCompletionValue = members.length;
let jobProgress = 0;
let jobSuccessful = 0;

let jobRate = 0;
let jobErrors = {};

const scrapeStart = new Date();

members.forEach((member) => {
  queue.add(() => processJob({ member, retries: 0 }));
});

async function processJob({ member, retries }) {
  try {
    const processStart = new Date().toISOString();

    const fetchStart = performance.now();
    const response = await customFetch(`https://www.bungie.net/Platform/Destiny2/${member.membershipType}/Profile/${member.membershipId}/?components=100,800,900`);
    const fetchEnd = performance.now();

    const computeStart = performance.now();
    const result = processResponse(member, response);
    const computeEnd = performance.now();

    jobProgress++;
    jobSuccessful++;

    jobRate++;

    if (result !== 'success') {
      jobErrors[result] = (jobErrors[result] ?? 1) + 1;

      if (retries < 3) {
        queue.add(() => processJob({ member, retries: retries + 1 }));
      }
    }

    return {
      error: result,
      member: {
        membershipType: member.membershipType,
        membershipId: member.membershipId,
      },
      performance: {
        start: processStart,
        fetch: fetchEnd - fetchStart,
        compute: computeEnd - computeStart,
      },
    };
  } catch (error) {
    jobRate++;
    jobProgress++;

    fs.promises.writeFile(`./logs/error.${member.membershipId}.${Date.now()}.txt`, `${JSON.stringify(member)}\n\n${typeof error}\n\n${error.toString()}\n\n${error.message}`);

    if (retries < 3) {
      queue.add(() => processJob({ member, retries: retries + 1 }));
    }

    return {
      error,
      member: {
        membershipType: member.membershipType,
        membershipId: member.membershipId,
      },
      performance: undefined,
    };
  }
}

function processResponse(member, response) {
  if (response && response.ErrorCode !== undefined) {
    if (response.ErrorCode === 1) {
      if (response.Response.profileRecords.data === undefined || Object.keys(response.Response.characterCollectibles.data).length === 0) {
        try {
          displayName = response.Response.profile.data?.userInfo.bungieGlobalDisplayName !== '' ? `${response.Response.profile.data?.userInfo.bungieGlobalDisplayName}#${response.Response.profile.data.userInfo.bungieGlobalDisplayNameCode.toString().padStart(4, '0')}` : response.Response.profile.data?.userInfo.displayName;
        } catch (e) {}

        if (process.env.STORE_JOB_RESULTS === 'true') {
          pool.query(mysql.format(`UPDATE braytech.members SET isPrivate = '1' WHERE membershipId = ?`, [member.membershipId]));
        }

        return 'private_profile';
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
          membershipType: member.membershipType,
          membershipId: member.membershipId,
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
                collectionsScore
              )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?
              )
            ON DUPLICATE KEY UPDATE displayName = ?, lastUpdated = ?, lastPlayed = ?, triumphScore = ?, legacyScore = ?, activeScore = ?, collectionsScore = ?`,
            [
              member.membershipType,
              member.membershipId,
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

      return 'success';
    } else if (response.ErrorCode !== undefined) {
      if (response.ErrorCode === 1601) {
        if (process.env.STORE_JOB_RESULTS === 'true') {
          pool.query(mysql.format(`DELETE FROM braytech.members WHERE membershipId = ?`, [member.membershipId]));
        }
      }

      return `bungie_${response.ErrorStatus}`;
    }
  }

  fs.promises.writeFile(`./logs/error.${member.membershipId}.${Date.now()}.txt`, `${JSON.stringify(member)}\n\n${response}`);

  return 'unknown_error';
}

// just in case
let finalising = false;

// relaying updates to the console/saving final statistics
async function updateLog() {
  const progress = Math.floor((jobProgress / jobCompletionValue) * 100);
  const timeElapsed = Math.ceil((Date.now() - scrapeStart.getTime()) / 60000);
  const timeRemaining = Math.floor((((Date.now() - scrapeStart.getTime()) / Math.max(jobProgress, 1)) * (jobCompletionValue - jobProgress)) / 60000);
  const timeComplete = new Date(Date.now() + ((Date.now() - scrapeStart.getTime()) / Math.max(jobProgress, 1)) * (jobCompletionValue - jobProgress));

  if (jobProgress === jobCompletionValue && finalising === false) {
    finalising = true;
    clearInterval(updateIntervalTimer);

    metrics = '';

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
      const rankQuery = mysql.format(
        `SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
      TRUNCATE leaderboards.ranks;
      INSERT INTO leaderboards.ranks (
          membershipType,
          membershipId,
          displayName,
          triumphScore,
          legacyScore,
          activeScore,
          collectionsScore,
          triumphRank,
          legacyRank,
          activeRank,
          collectionsRank
        ) (
          SELECT membershipType,
            membershipId,
            displayName,
            triumphScore,
            legacyScore,
            activeScore,
            collectionsScore,
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
                  ORDER BY collectionsScore DESC
                ) collectionsRank
              FROM profiles.members
              WHERE lastUpdated >= ? AND lastPlayed > '2023-02-28 17:00:00' 
              ORDER BY displayName ASC
            ) R
        ) ON DUPLICATE KEY
      UPDATE displayName = R.displayName,
        triumphScore = R.triumphScore,
        legacyScore = R.legacyScore,
        activeScore = R.activeScore,
        collectionsScore = R.collectionsScore,
        triumphRank = R.triumphRank,
        legacyRank = R.legacyRank,
        activeRank = R.activeRank,
        collectionsRank = R.collectionsRank;
      COMMIT;`,
        [scrapeStart]
      );

      const statsTriumphsQuery = mysql.format(`INSERT INTO profiles.commonality (date, hash, value) VALUES ?;`, [Object.entries(StatsTriumphs).map(([hash, value]) => [scrapeStart, hash, value])]);
      const statsCollectiblesQuery = mysql.format(`INSERT INTO profiles.commonality (date, hash, value) VALUES ?;`, [Object.entries(StatsCollections).map(([hash, value]) => [scrapeStart, hash, value])]);

      await fs.promises.writeFile(`./temp/queries.${Date.now()}.sql`, `${scrapesStatusQuery}\n\n${rankQuery}`);
      await fs.promises.writeFile(`./temp/queries.extended.${Date.now()}.sql`, `${statsTriumphsQuery}\n\n${statsCollectiblesQuery}`);

      console.log('Save scrape to profiles.scrapes...');
      const [status] = await pool.query(scrapesStatusQuery);
      console.log('Saved scrape profiles.scrapes...');

      console.log('Evaluate leaderboards...');
      const ranks = await pool.query(rankQuery);
      console.log(ranks);
      console.log('Evaluated leaderboards...');

      console.log('Save Triumphs stats to database...');
      await pool.query(statsTriumphsQuery);
      console.log('Saved Triumphs stats to database...');

      console.log('Save Collections stats to database...');
      await pool.query(statsCollectiblesQuery);
      console.log('Saved Collections stats to database...');

      console.log('Cache commonality...');
      await fetch(`http://0.0.0.0:8080/Generate/Commonality?id=${status.insertId}`, {
        headers: {
          'x-api-key': 'insomnia',
        },
      });
      console.log('Cached commonality...');
      console.log('Generate common...');
      await fetch('http://0.0.0.0:8080/Generate', {
        headers: {
          'x-api-key': 'insomnia',
        },
      });
      console.log('Generated common...');
    }

    process.exit();
  } else {
    metrics = `voluspa_scraper_progress ${progress}\n\nvoluspa_scraper_job_rate ${jobRate}\n\nvoluspa_scraper_job_progress ${jobProgress}\n\nvoluspa_scraper_job_completion_value ${jobCompletionValue}\n\nvoluspa_scraper_queue_active ${queue.pending}\n\nvoluspa_scraper_queue_pending ${queue.size}\n\nvoluspa_scraper_job_parallel_programs ${StatsParallelProgram.length}\n\nvoluspa_scraper_job_time_remaining ${Math.max((timeComplete.getTime() - Date.now()) / 1000, 0)}\n\n${Object.keys(jobErrors)
      .map((key) => `voluspa_scraper_job_error_${key} ${jobErrors[key]}`)
      .join('\n\n')}`;

    jobRate = 0;
    Object.keys(jobErrors).forEach((key) => {
      jobErrors[key] = 0;
    });

    await Promise.all([
      fs.promises.writeFile('./temp/triumphs.temp.json', JSON.stringify(StatsTriumphs)), //
      fs.promises.writeFile('./temp/collections.temp.json', JSON.stringify(StatsCollections)),
      fs.promises.writeFile('./temp/parallel-program.temp.json', JSON.stringify(StatsParallelProgram)),
    ]);
  }

  console.table({
    Progress: progress,
    JobProgress: jobProgress,
    JobCompletionValue: jobCompletionValue,
    QueueActive: queue.pending,
    QueuePending: queue.size,
    TimeElapsed: timeElapsed,
    TimeRemaining: timeRemaining,
    TimeComplete: timeComplete.toLocaleString('en-AU', { dateStyle: 'full', timeStyle: 'long', hour12: false, timeZone: 'Australia/Brisbane' }),
    ParallelPrograms: StatsParallelProgram.length,
  });
}

const updateIntervalTimer = setInterval(updateLog, 5000);

const queueResults = await queue.onIdle();

await fs.promises.writeFile(`./logs/job-results.${Date.now()}.json`, JSON.stringify(queueResults.filter((job) => job.error !== 'success')));
console.log('Saved job results to disk');