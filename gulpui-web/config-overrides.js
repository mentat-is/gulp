const path = require('path');
const { override, addWebpackAlias, addWebpackResolve } = require('customize-cra');

module.exports = override(
  addWebpackAlias({
    ['@']: path.resolve(__dirname, 'src')
  }),
  addWebpackResolve({
    fallback: {
      "events": require.resolve("events/")
    }
  })
);
