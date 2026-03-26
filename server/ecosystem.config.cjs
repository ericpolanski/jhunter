module.exports = {
  apps: [{
    name: 'jhunter-server',
    script: './index.js',
    instances: 1,
    autorestart: true,
    max_restarts: 10,
    min_uptime: 5000,
    max_memory_restart: '500M',
    listen_timeout: 8000,
    kill_timeout: 5000,
    env: {
      NODE_ENV: 'production'
    }
  }]
};
