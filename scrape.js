import fs from 'fs';
import http from 'http';
import dotenv from 'dotenv';
import mysql from 'mysql2';
import pQueue from 'p-queue';

import { customFetch } from './requestUtils.js';
import { basic, seals } from './dataUtils.js';

dotenv.config();

console.log('VOLUSPA');

let metrics = '';
const storeScrapeResults = process.env.STORE_JOB_RESULTS === 'true';

const requestListener = function (request, response) {
  response.writeHead(200);
  response.end(metrics);
};

const server = http.createServer(requestListener);

server.listen(8181, '0.0.0.0', () => {
  console.log(`HTTP server started`);
});

const queue = new pQueue({ concurrency: 180 });

// setup basic db stuff
const puddle = mysql.createPool({
  host: process.env.MYSQL_HOST,
  port: process.env.MYSQL_PORT,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  connectionLimit: 10,
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

const results = [];

async function processJob({ member, retries }) {
  try {
    const processStart = new Date().toISOString();

    const fetchStart = performance.now();
    const response = await customFetch(`https://www.bungie.net/Platform/Destiny2/${member.membershipType}/Profile/${member.membershipId}/?components=100,800,900`);
    const fetchEnd = performance.now();

    const computeStart = performance.now();
    const result = processResponse(member, response);
    const computeEnd = performance.now();

    jobRate++;

    if (result !== 'success') {
      jobErrors[result] = (jobErrors[result] ?? 1) + 1;
    }

    if (result !== 'success' && retries < 3) {
      queue.add(() => processJob({ member, retries: retries + 1 }));
    } else {
      jobProgress++;
      jobSuccessful++;

      results.push({
        error: result,
        member: {
          membershipType: member.membershipType,
          membershipId: member.membershipId,
        },
        retries,
        performance: {
          start: processStart,
          fetch: fetchEnd - fetchStart,
          compute: computeEnd - computeStart,
        },
      });
    }
  } catch (error) {
    console.log(error);
    fs.promises.writeFile(`./logs/error.${member.membershipId}.${Date.now()}.txt`, `${JSON.stringify(member)}\n\n${typeof error}\n\n${error.toString()}\n\n${error.message}`);

    if (retries < 3) {
      queue.add(() => processJob({ member, retries: retries + 1 }));
    } else {
      jobRate++;
      jobProgress++;

      results.push({
        error,
        member: {
          membershipType: member.membershipType,
          membershipId: member.membershipId,
        },
        retries,
        performance: undefined,
      });
    }
  }
}

function processResponse(member, response) {
  if (response && response.ErrorCode !== undefined) {
    if (response.ErrorCode === 1) {
      if (response.Response.profileRecords.data === undefined || Object.keys(response.Response.characterCollectibles.data).length === 0) {
        if (storeScrapeResults) {
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
      const PreparedValues = basic(response);

      const date = new Date();

      if (storeScrapeResults) {
        pool.query(
          mysql.format(
            `INSERT INTO profiles.members (
                membershipType,
                membershipId,
                displayName,
                lastUpdated,
                lastPlayed,
                legacyScore,
                activeScore,
                collectionScore
              )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?
              )
            ON DUPLICATE KEY UPDATE
              displayName = VALUES(displayName),
              lastUpdated = VALUES(lastUpdated),
              lastPlayed = VALUES(lastPlayed),
              legacyScore = VALUES(legacyScore),
              activeScore = VALUES(activeScore),
              collectionScore = VALUES(collectionScore)
            `,
            [
              member.membershipType, //
              member.membershipId,
              PreparedValues.displayName,
              date,
              PreparedValues.lastPlayed,
              PreparedValues.legacyScore,
              PreparedValues.activeScore,
              collections.length,
            ]
          )
        );
        pool.query(
          mysql.format(
            `INSERT INTO profiles.members_seals (
                date,
                membershipId,
                presentationNodeHash,
                completionRecordState,
                completionDate,
                gildingRecordState,
                gildingDate,
                gildingCompletedCount
              )
            VALUES ?
            ON DUPLICATE KEY UPDATE
              date = VALUES(date),
              completionRecordState = COALESCE(VALUES(completionRecordState), completionRecordState),
              completionDate = COALESCE(completionDate, VALUES(completionDate)),
              gildingRecordState = COALESCE(VALUES(gildingRecordState), gildingRecordState),
              gildingDate = COALESCE(gildingDate, VALUES(gildingDate)),
              gildingCompletedCount = COALESCE(VALUES(gildingCompletedCount), gildingCompletedCount)
            `,
            [
              seals(response).map(
                ([
                  presentationNodeHash, //
                  completionRecordState,
                  gildingRecordState,
                  gildingCompletedCount,
                ]) => [
                  scrapeStart, //
                  member.membershipId,
                  presentationNodeHash,
                  completionRecordState,
                  ((completionRecordState ?? 4) & 4) === 0 ? date : null,
                  gildingRecordState,
                  ((gildingRecordState ?? 4) & 4) === 0 ? date : null,
                  gildingCompletedCount,
                ]
              ),
            ]
          )
        );
      }

      return 'success';
    } else if (response.ErrorCode !== undefined) {
      if (response.ErrorCode === 1601) {
        if (storeScrapeResults) {
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

    if (storeScrapeResults) {
      const scrapesStatusQuery = mysql.format(`INSERT INTO profiles.scrapes (date, duration, crawled, assessed) VALUES (?, ?, ?, ?);`, [scrapeStart, Math.ceil((Date.now() - scrapeStart.getTime()) / 60000), jobCompletionValue, jobSuccessful]);
      const rankQuery = mysql.format(
        `SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        TRUNCATE leaderboards.ranks;
        
        INSERT INTO leaderboards.ranks (
              membershipType,
              membershipId,
              displayName,
              activeScore,
              sealScore,
              gildScore,
              legacyScore,
              collectionScore,
              activeRank,
              sealRank,
              gildRank,
              legacyRank,
              collectionRank
          ) (
              SELECT R.membershipType,
                R.membershipId,
                R.displayName,
                R.activeScore,
                R.sealScore,
                R.gildScore,
                R.legacyScore,
                R.collectionScore,
                R.activeRank,
                R.sealRank,
                R.gildRank,
                R.legacyRank,
                R.collectionRank
              FROM (
                    SELECT members.*,
                      members_seals.sealScore,
                      members_seals.gildScore,
                      DENSE_RANK() OVER (
                          ORDER BY members.legacyScore DESC
                      ) AS legacyRank,
                      DENSE_RANK() OVER (
                          ORDER BY members.activeScore DESC
                      ) AS activeRank,
                      DENSE_RANK() OVER (
                          ORDER BY members.collectionScore DESC
                      ) AS collectionRank,
                      DENSE_RANK() OVER (
                          ORDER BY members_seals.sealScore DESC
                      ) AS sealRank,
                      DENSE_RANK() OVER (
                          ORDER BY members_seals.gildScore DESC
                      ) AS gildRank
                    FROM profiles.members AS members
                      JOIN (
                          SELECT membershipId,
                            COUNT(
                                CASE
                                  WHEN (completionRecordState & 4) = 0 THEN 1
                                END
                            ) AS sealScore,
                            SUM(gildingCompletedCount) AS gildScore
                          FROM profiles.members_seals as s
                          GROUP BY membershipId
                      ) AS members_seals ON members.membershipId = members_seals.membershipId
                    WHERE members.lastUpdated >= ?
                      AND members.lastPlayed > '2023-02-28 17:00:00'
                    ORDER BY members.displayName ASC
                ) as R
          ) ON DUPLICATE KEY
        UPDATE displayName = R.displayName,
          activeScore = R.activeScore,
          sealScore = R.sealScore,
          gildScore = R.gildScore,
          legacyScore = R.legacyScore,
          collectionScore = R.collectionScore,
          activeRank = R.activeRank,
          sealRank = R.sealRank,
          gildRank = R.gildRank,
          legacyRank = R.legacyRank,
          collectionRank = R.collectionRank;
        
        UPDATE leaderboards.ranks r
          INNER JOIN (
              SELECT membershipId,
                ROW_NUMBER() OVER (
                    ORDER BY activeRank,
                      displayName
                ) AS activePosition,
                ROW_NUMBER() OVER (
                    ORDER BY sealRank,
                      displayName
                ) AS sealPosition,
                ROW_NUMBER() OVER (
                    ORDER BY gildRank,
                      displayName
                ) AS gildPosition,
                ROW_NUMBER() OVER (
                    ORDER BY legacyRank,
                      displayName
                ) AS legacyPosition,
                ROW_NUMBER() OVER (
                    ORDER BY collectionRank,
                      displayName
                ) AS collectionPosition
              FROM leaderboards.ranks
          ) p ON p.membershipId = r.membershipId
        SET r.activePosition = p.activePosition,
          r.sealPosition = p.sealPosition,
          r.gildPosition = p.gildPosition,
          r.legacyPosition = p.legacyPosition,
          r.collectionPosition = p.collectionPosition;
        
        UPDATE leaderboards.ranks r
          INNER JOIN (
             SELECT membershipId,
                ROUND(
                   PERCENT_RANK() OVER (
                      ORDER BY activeScore DESC
                   ),
                   2
                ) activePercentile,
                ROUND(
                   PERCENT_RANK() OVER (
                      ORDER BY sealScore DESC
                   ),
                   2
                ) sealPercentile,
                ROUND(
                   PERCENT_RANK() OVER (
                      ORDER BY gildScore DESC
                   ),
                   2
                ) gildPercentile,
                ROUND(
                   PERCENT_RANK() OVER (
                      ORDER BY legacyScore DESC
                   ),
                   2
                ) legacyPercentile,
                ROUND(
                   PERCENT_RANK() OVER (
                      ORDER BY collectionScore DESC
                   ),
                   2
                ) collectionPercentile
             FROM leaderboards.ranks
          ) p ON p.membershipId = r.membershipId
        SET r.activePercentile = p.activePercentile,
          r.sealPercentile = p.sealPercentile,
          r.gildPercentile = p.gildPercentile,
          r.legacyPercentile = p.legacyPercentile,
          r.collectionPercentile = p.collectionPercentile;
        
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
          'x-api-key': process.env.VOLUSPA_API_KEY,
        },
      });
      console.log('Cached commonality...');
    }

    process.exit();
  } else {
    metrics = `voluspa_scraper_progress ${progress}\n\nvoluspa_scraper_job_rate ${jobRate}\n\nvoluspa_scraper_job_progress ${jobProgress}\n\nvoluspa_scraper_job_completion_value ${jobCompletionValue}\n\nvoluspa_scraper_queue_active ${queue.pending}\n\nvoluspa_scraper_queue_pending ${queue.size}\n\nvoluspa_scraper_job_time_remaining ${Math.max((timeComplete.getTime() - Date.now()) / 1000, 0)}\n\n${Object.keys(jobErrors)
      .map((key) => `voluspa_scraper_job_error_${key} ${jobErrors[key]}`)
      .join('\n\n')}`;

    jobRate = 0;
    Object.keys(jobErrors).forEach((key) => {
      jobErrors[key] = 0;
    });
  }
}

const updateIntervalTimer = setInterval(updateLog, 5000);

await queue.onIdle();
await fs.promises.writeFile(`./logs/job-results.${Date.now()}.json`, JSON.stringify(results.filter((job) => job.error !== 'success')));
console.log('Saved job results to disk');
