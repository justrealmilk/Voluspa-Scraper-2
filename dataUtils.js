// import fs from 'fs';

// const manifest = await fs.promises
//   .readFile(`../voluspa/cache/manifest/state.json`, 'utf8')
//   .then((string) => JSON.parse(string))
//   .then((state) => fs.promises.readFile(`../voluspa/cache/manifest/versions/${state.version}/definitions.json`, 'utf8'))
//   .then((string) => JSON.parse(string));

// console.log([...manifest.DestinyPresentationNodeDefinition[616318467].children.presentationNodes, ...manifest.DestinyPresentationNodeDefinition[1881970629].children.presentationNodes].map(({ presentationNodeHash }) => ({
//   presentationNodeHash,
//   completionRecordHash: manifest.DestinyPresentationNodeDefinition[presentationNodeHash].completionRecordHash,
//   gildingTrackingRecordHash: manifest.DestinyRecordDefinition[manifest.DestinyPresentationNodeDefinition[presentationNodeHash].completionRecordHash!].titleInfo.gildingTrackingRecordHash,
//   scope: manifest.DestinyRecordDefinition[manifest.DestinyPresentationNodeDefinition[presentationNodeHash].completionRecordHash!].scope
// })))

// const sealPresentationNodeHashes = [...manifest.DestinyPresentationNodeDefinition[616318467].children.presentationNodes, ...manifest.DestinyPresentationNodeDefinition[1881970629].children.presentationNodes];

const sealPresentationNodeHashes = [
  {
    presentationNodeHash: 1210906309,
    completionRecordHash: 3175660257,
    scope: 1,
  },
  {
    presentationNodeHash: 1270675703,
    completionRecordHash: 4083696547,
    scope: 0,
  },
  {
    presentationNodeHash: 1270675702,
    completionRecordHash: 2269203216,
    scope: 0,
  },
  {
    presentationNodeHash: 1270675701,
    completionRecordHash: 3570567217,
    scope: 0,
  },
  {
    presentationNodeHash: 1270675700,
    completionRecordHash: 1722592950,
    scope: 0,
  },
  {
    presentationNodeHash: 1021469803,
    completionRecordHash: 1142693639,
    scope: 0,
  },
  {
    presentationNodeHash: 238107129,
    completionRecordHash: 865076293,
    scope: 0,
  },
  {
    presentationNodeHash: 4186496383,
    completionRecordHash: 3906538939,
    scope: 0,
  },
  {
    presentationNodeHash: 1976056830,
    completionRecordHash: 2889189256,
    scope: 0,
  },
  {
    presentationNodeHash: 1317417718,
    completionRecordHash: 3646306576,
    gildingTrackingRecordHash: 1655448318,
    scope: 0,
  },
  {
    presentationNodeHash: 3896035657,
    completionRecordHash: 2126152885,
    gildingTrackingRecordHash: 908738851,
    scope: 0,
  },
  {
    presentationNodeHash: 1705744655,
    completionRecordHash: 3974717227,
    scope: 0,
  },
  {
    presentationNodeHash: 4183969062,
    completionRecordHash: 2302993504,
    scope: 0,
  },
  {
    presentationNodeHash: 2592822840,
    completionRecordHash: 1089543274,
    gildingTrackingRecordHash: 2981294724,
    scope: 0,
  },
  {
    presentationNodeHash: 475207334,
    completionRecordHash: 969142496,
    gildingTrackingRecordHash: 2228586830,
    scope: 0,
  },
  {
    presentationNodeHash: 2613142083,
    completionRecordHash: 3910736783,
    scope: 0,
  },
  {
    presentationNodeHash: 3598951881,
    completionRecordHash: 3056675381,
    gildingTrackingRecordHash: 3417514659,
    scope: 0,
  },
  {
    presentationNodeHash: 2594486939,
    completionRecordHash: 1228693527,
    scope: 0,
  },
  {
    presentationNodeHash: 854126634,
    completionRecordHash: 3097916612,
    scope: 0,
  },
  {
    presentationNodeHash: 2161171268,
    completionRecordHash: 1564001702,
    gildingTrackingRecordHash: 2561695992,
    scope: 0,
  },
  {
    presentationNodeHash: 3061040177,
    completionRecordHash: 2489106733,
    scope: 0,
  },
  {
    presentationNodeHash: 2886738008,
    completionRecordHash: 1971228746,
    scope: 0,
  },
  {
    presentationNodeHash: 2185719388,
    completionRecordHash: 3588818798,
    scope: 0,
  },
  {
    presentationNodeHash: 361765966,
    completionRecordHash: 1438167672,
    gildingTrackingRecordHash: 4141599814,
    scope: 0,
  },
  {
    presentationNodeHash: 1733555826,
    completionRecordHash: 3298130972,
    gildingTrackingRecordHash: 2506618338,
    scope: 0,
  },
  {
    presentationNodeHash: 3776992251,
    completionRecordHash: 3464275895,
    gildingTrackingRecordHash: 1715149073,
    scope: 0,
  },
  {
    presentationNodeHash: 3665267419,
    completionRecordHash: 1556658903,
    gildingTrackingRecordHash: 1249847601,
    scope: 0,
  },
  {
    presentationNodeHash: 79180995,
    completionRecordHash: 2482004751,
    scope: 0,
  },
  {
    presentationNodeHash: 3734352323,
    completionRecordHash: 4141971983,
    scope: 0,
  },
  {
    presentationNodeHash: 2960810718,
    completionRecordHash: 540377256,
    scope: 0,
  },
  {
    presentationNodeHash: 379405979,
    completionRecordHash: 2584970263,
    scope: 0,
  },
  {
    presentationNodeHash: 1827854727,
    completionRecordHash: 2909250963,
    scope: 0,
  },
  {
    presentationNodeHash: 560097044,
    completionRecordHash: 3214425110,
    scope: 0,
  },
  {
    presentationNodeHash: 1486062207,
    completionRecordHash: 1384029371,
    scope: 0,
  },
  {
    presentationNodeHash: 1210906308,
    completionRecordHash: 3249408038,
    scope: 0,
  },
  {
    presentationNodeHash: 1321008452,
    completionRecordHash: 4250626982,
    scope: 0,
  },
  {
    presentationNodeHash: 1321008453,
    completionRecordHash: 4176879201,
    scope: 0,
  },
  {
    presentationNodeHash: 1321008458,
    completionRecordHash: 3947410852,
    scope: 0,
  },
  {
    presentationNodeHash: 1321008459,
    completionRecordHash: 1710217127,
    scope: 0,
  },
  {
    presentationNodeHash: 2107507397,
    completionRecordHash: 1343839969,
    scope: 0,
  },
  {
    presentationNodeHash: 1210906311,
    completionRecordHash: 1284946259,
    scope: 0,
  },
  {
    presentationNodeHash: 1321008456,
    completionRecordHash: 2991743002,
    scope: 0,
  },
  {
    presentationNodeHash: 1321008457,
    completionRecordHash: 2796658869,
    scope: 0,
  },
  {
    presentationNodeHash: 1321008462,
    completionRecordHash: 1087927672,
    scope: 0,
  },
  {
    presentationNodeHash: 1321008463,
    completionRecordHash: 1561715947,
    scope: 0,
  },
  {
    presentationNodeHash: 1210906310,
    completionRecordHash: 1109459264,
    scope: 0,
  },
  {
    presentationNodeHash: 1321008460,
    completionRecordHash: 3169895614,
    scope: 0,
  },
  {
    presentationNodeHash: 1321008461,
    completionRecordHash: 2499679097,
    scope: 0,
  },
  {
    presentationNodeHash: 955166375,
    completionRecordHash: 1185680627,
    scope: 0,
  },
  {
    presentationNodeHash: 955166374,
    completionRecordHash: 1866578144,
    scope: 0,
  },
  {
    presentationNodeHash: 1194128730,
    completionRecordHash: 966207508,
    scope: 0,
  },
  {
    presentationNodeHash: 717225803,
    completionRecordHash: 2056461735,
    scope: 0,
  },
  {
    presentationNodeHash: 2978379966,
    completionRecordHash: 2472740040,
    scope: 0,
  },
  {
    presentationNodeHash: 450166688,
    completionRecordHash: 317521250,
    scope: 0,
  },
  {
    presentationNodeHash: 1106177979,
    completionRecordHash: 758645239,
    scope: 0,
  },
  {
    presentationNodeHash: 3680676656,
    completionRecordHash: 3766199186,
    scope: 0,
  },
  {
    presentationNodeHash: 581214566,
    completionRecordHash: 4167244320,
    scope: 0,
  },
  {
    presentationNodeHash: 3212358005,
    completionRecordHash: 2980266417,
    scope: 0,
  },
  {
    presentationNodeHash: 1376640684,
    completionRecordHash: 2226626398,
    scope: 0,
  },
  {
    presentationNodeHash: 1276693937,
    completionRecordHash: 2126548397,
    scope: 0,
  },
  {
    presentationNodeHash: 3251218484,
    completionRecordHash: 2284880502,
    scope: 0,
  },
  {
    presentationNodeHash: 2086100423,
    completionRecordHash: 2072890963,
    scope: 0,
  },
];

export function basic(response) {
  const membershipType = response.Response.profile.data.userInfo.membershipType;
  const membershipId = response.Response.profile.data.userInfo.membershipId;
  const displayName = response.Response.profile.data?.userInfo.bungieGlobalDisplayName !== '' ? `${response.Response.profile.data?.userInfo.bungieGlobalDisplayName}#${response.Response.profile.data.userInfo.bungieGlobalDisplayNameCode.toString().padStart(4, '0')}` : response.Response.profile.data?.userInfo.displayName.slice(0, 32);

  const lastPlayed = new Date(response.Response.profile.data.dateLastPlayed);

  return {
    membershipType,
    membershipId,
    displayName,
    lastPlayed: lastPlayed.getTime() > 10000 ? lastPlayed : null,
    legacyScore: response.Response.profileRecords.data.legacyScore,
    activeScore: response.Response.profileRecords.data.activeScore,
  };
}

export function defaultCharacterId(response) {
  try {
    if (response.Response.characterActivities?.data !== undefined) {
      const characterIds = response.Response.profile.data.characterIds.map((characterId) => [characterId, response.Response.characterActivities.data[characterId]]).sort((a, b) => new Date(b[1].dateActivityStarted).getTime() - new Date(a[1].dateActivityStarted).getTime());

      return characterIds[0][0];
    } else {
      const characterIds = response.Response.profile.data.characterIds.sort((a, b) => new Date(response.Response.characters.data[b].dateLastPlayed).getTime() - new Date(response.Response.characters.data[a].dateLastPlayed).getTime());

      return characterIds[0];
    }
  } catch (error) {
    return undefined;
  }
}

function recordComponent(response, hash, scope) {
  const characterId = response.Response.profile.data.characterIds[0];

  const record = scope === 1 ? response.Response.characterRecords.data[characterId].records[hash] : response.Response.profileRecords.data.records[hash];

  return record;
}

function getProgessFromComponent(record) {
  return record?.objectives?.[0]?.progress ?? record?.intervalObjectives?.[0]?.progress;
}

function metricComponent(response, hash) {
  return response.Response.metrics.data.metrics[hash];
}

export function seals(response) {
  const characterId = response.Response.profile.data.characterIds[0];

  if (characterId !== undefined) {
    const states = [];

    for (const { presentationNodeHash, completionRecordHash, gildingTrackingRecordHash, scope } of sealPresentationNodeHashes) {
      const completionRecord = recordComponent(response, completionRecordHash, scope);
      const gildingRecord = recordComponent(response, gildingTrackingRecordHash, scope);

      states.push([
        presentationNodeHash, //
        completionRecord.state,
        gildingRecord?.state,
        gildingRecord?.completedCount,
      ]);
    }

    return states;
  } else {
    return null;
  }
}
