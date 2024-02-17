// import fs from 'fs';

// const manifest = await fs.promises
//   .readFile(`../voluspa/cache/manifest/state.json`, 'utf8')
//   .then((string) => JSON.parse(string))
//   .then((state) => fs.promises.readFile(`../voluspa/cache/manifest/versions/${state.version}/definitions.json`, 'utf8'))
//   .then((string) => JSON.parse(string));

// const sealPresentationNodeHashes = [...manifest.DestinyPresentationNodeDefinition[616318467].children.presentationNodes, ...manifest.DestinyPresentationNodeDefinition[1881970629].children.presentationNodes];
const sealPresentationNodeHashes = [
  {
    presentationNodeHash: 1210906309,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1270675703,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1270675702,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1270675701,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1270675700,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1021469803,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 238107129,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 4186496383,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1976056830,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1317417718,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 3896035657,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1705744655,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 4183969062,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 2592822840,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 475207334,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 2613142083,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 3598951881,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 2594486939,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 854126634,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 2161171268,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 3061040177,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 2886738008,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 2185719388,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 361765966,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1733555826,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 3776992251,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 3665267419,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 79180995,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 3734352323,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 2960810718,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 379405979,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1827854727,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 560097044,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1486062207,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1210906308,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1321008452,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1321008453,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1321008458,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1321008459,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 2107507397,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1210906311,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1321008456,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1321008457,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1321008462,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1321008463,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1210906310,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1321008460,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1321008461,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 955166375,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 955166374,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1194128730,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 717225803,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 2978379966,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 450166688,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1106177979,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 3680676656,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 581214566,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 3212358005,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1376640684,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 1276693937,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 3251218484,
    nodeDisplayPriority: 0,
  },
  {
    presentationNodeHash: 2086100423,
    nodeDisplayPriority: 0,
  },
];

export function values(response) {
  const membershipType = response.Response.profile.data.userInfo.membershipType;
  const membershipId = response.Response.profile.data.userInfo.membershipId;
  const displayName = response.Response.profile.data?.userInfo.bungieGlobalDisplayName !== '' ? `${response.Response.profile.data?.userInfo.bungieGlobalDisplayName}#${response.Response.profile.data.userInfo.bungieGlobalDisplayNameCode.toString().padStart(4, '0')}` : response.Response.profile.data?.userInfo.displayName.slice(0, 32);

  let lastPlayed = new Date(response.Response.profile.data.dateLastPlayed);

  if (lastPlayed.getTime() < 10000) {
    lastPlayed = null;
  }

  return {
    membershipType,
    membershipId,
    displayName,
    lastPlayed,
    legacyScore: response.Response.profileRecords.data.legacyScore,
    activeScore: response.Response.profileRecords.data.activeScore,
    seals: seals(response),
  };
}

export function defaultCharacterId(response) {
  try {
    if (response.Response.characterActivities?.data !== undefined) {
      const characterIds = response.Response.profile.data.characterIds.map((characterId) => [characterId, response.Response.characterActivities.data[characterId]]).sort((a, b) => new Date(b[1].dateActivityStarted).getTime() - new Date(a[1].dateActivityStarted).getTime());

      return characterIds[0][0];
    } else {
      const characterIds = [...response.Response.profile.data.characterIds].sort((a, b) => new Date(response.Response.characters.data[b].dateLastPlayed).getTime() - new Date(response.Response.characters.data[a].dateLastPlayed).getTime());

      return characterIds[0];
    }
  } catch (error) {
    return undefined;
  }
}

function seals(response) {
  const characterId = defaultCharacterId(response);

  if (characterId !== undefined) {
    const state = [];

    for (const { presentationNodeHash } of sealPresentationNodeHashes) {
      const completionRecordHash = manifest.DestinyPresentationNodeDefinition[presentationNodeHash].completionRecordHash;

      if (completionRecordHash !== undefined) {
        const record = manifest.DestinyRecordDefinition[completionRecordHash].scope === 1 ? response.Response.characterRecords.data[characterId].records[completionRecordHash] : response.Response.profileRecords.data.records[completionRecordHash];

        state.push(`[${presentationNodeHash},${record.state}]`);
      }
    }

    return `[${state.join(',')}]`;
  } else {
    return null;
  }
}
