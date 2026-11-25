const path = require('path');
const { override, addWebpackAlias, addWebpackResolve } = require('customize-cra');

module.exports = override(
  addWebpackAlias({
    ['@']: path.resolve(__dirname, 'src'),
    ['@gulp/ui-plugin-sdk/v1$']: path.resolve(__dirname, 'src/plugin-api/v1')
  }),
  addWebpackResolve({
    fallback: {
      "events": require.resolve("events/")
    }
  })
);
