# Security policy

## Keep credentials out of Git

Never commit or upload:

- Telegram `API_HASH`, `BOT_TOKEN`, or session strings
- `.env` files
- Browser cookie exports
- SQLite databases
- Private keys or deployment credentials

Use the secret manager provided by the hosting platform. The included
`.gitignore` and `.dockerignore` exclude the common sensitive files.

## Reporting a vulnerability

Do not open a public issue with credentials, session data, or an exploit.
Contact the repository owner privately through the security contact configured
on GitHub.