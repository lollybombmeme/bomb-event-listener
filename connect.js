const Sentry = require("@sentry/node");
const Redis = require("ioredis");

const config = require("./config");
const logger = require("./helper/logger")

const redisClient = new Redis(config.REDIS_URL);
redisClient.on("error", (err) => console.log("Redis Server Error", err));

if (config.SENTRY_DSN) {
  Sentry.init({
    dsn: config.SENTRY_DSN,
    tracesSampleRate: 1.0,
    debug: true
  });
  logger.info(`Init sentry: ${config.SENTRY_DSN}`)
}

module.exports = {
  redisClient
};
