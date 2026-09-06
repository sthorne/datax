package version

// Release is the software version of this tree, semantic-versioned and
// bumped in the pull request that changes behavior: the minor for a new
// capability, the patch for a fix. It is distinct from the cluster
// (protocol) version above: many releases share one protocol version,
// and a release that introduces a new protocol version says so in the
// changelog. The build workflow stamps binaries with an exact git tag
// when one matches, and with "v<Release>+<commit>" otherwise; `datax
// version` prints the stamp along with the protocol version the binary
// speaks. CHANGELOG.md at the repository root records what each release
// carries.
const Release = "0.48.0"
