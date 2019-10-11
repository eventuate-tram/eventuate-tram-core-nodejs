const getEnvVar = envVar => process.env[ envVar ];

/**
 *
 * @param envVars
 * @param fallbackValues
 * @returns {String[]}
 */
function ensureEnvVariables(envVars, fallbackValues) {
  const hitsAndMisses = Array.from(envVars).map((envVar, idx) => ({
    env: envVar,
    val: getEnvVar(envVar),
    fallback: fallbackValues && fallbackValues[ idx ]
  }));
  const misses = hitsAndMisses.filter(({ val, fallback }) => !val && (typeof fallback === 'undefined')).map(({ env }) => env);
  if (misses.length) {
    throw new Error(`Set up these environment variables: ${ misses.join(', ') }`);
  }
  return hitsAndMisses.map(({ val, fallback }) => (val || fallback));
}

function ensureEnvVariable(envVar, fallback) {
  const [ result ] = ensureEnvVariables([ envVar ], [ fallback ]);
  return result;
}

module.exports = {
  ensureEnvVariable,
  ensureEnvVariables
};
