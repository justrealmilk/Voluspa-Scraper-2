export function values(response) {
  const membershipType = response.Response.profile.data.userInfo.membershipType;
  const membershipId = response.Response.profile.data.userInfo.membershipId;
  const displayName = response.Response.profile.data.userInfo.displayName;

  let lastPlayed = new Date(response.Response.profile.data.dateLastPlayed);

  if (lastPlayed.getTime() < 10000) {
    lastPlayed = null;
  }

  return {
    membershipType,
    membershipId,
    displayName,
    lastPlayed,
    triumphScore: response.Response.profileRecords.data.lifetimeScore,
    legacyScore: response.Response.profileRecords.data.legacyScore,
    activeScore: response.Response.profileRecords.data.activeScore,
  };
}
