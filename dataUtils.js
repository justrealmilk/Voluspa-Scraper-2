import fs from 'fs';

const manifest = await fs.promises
  .readFile(`../voluspa/cache/manifest/state.json`, 'utf8')
  .then((string) => JSON.parse(string))
  .then((state) => fs.promises.readFile(`../voluspa/cache/manifest/versions/${state.version}/definitions.json`, 'utf8'))
  .then((string) => JSON.parse(string));

const sealPresentationNodeHashes = [...manifest.DestinyPresentationNodeDefinition[616318467].children.presentationNodes, ...manifest.DestinyPresentationNodeDefinition[1881970629].children.presentationNodes];

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
