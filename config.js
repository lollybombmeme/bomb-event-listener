require("dotenv").config();

class Config {
    REDIS_URL = process.env.REDIS_URL;
    CELERY_BROKER_URL = process.env.CELERY_BROKER_URL;
    CELERY_ROUTES = {
        "worker.on_bridge_token": "bomb-bridge-queue",
        "worker.on_claim_token": "bomb-bridge-queue",
    };
    SENTRY_DSN = process.env.SENTRY_DSN;
}

module.exports = new Config();
