# Kesher chat relay server

A small Node.js WebSocket server for the "קֶשֶׁר" chat app: sign in with just
a display name (no phone number), add contacts, create groups, and chat in
DMs, groups, or one shared community/global chat. Everything is saved to a
`data.json` file next to the server so history survives reconnects and
restarts (as long as Render's disk for this instance isn't wiped by a fresh
deploy — see note below).

## ⚠️ This replaces the SuperBoth game relay

This server.js is a different app from the SuperBoth 1v1 game relay you
had running at `server-check-g7wn.onrender.com`. A Render Web Service only
runs one `server.js`, so deploying this **will replace** the game relay on
that URL — the game will stop working there.

**Recommended:** create a *new, separate* Render Web Service for the chat
app (Render's free tier allows multiple services), so your existing game
server keeps working untouched. You'll get a different URL, e.g.
`kesher-chat.onrender.com`, which you then plug into the chat app.

If you're fine losing the game relay on this URL and want to reuse it,
that also works — just overwrite the repo's contents with these 3 files.

## Deploy on Render

1. Push this folder to a GitHub repo (or use Render's "public Git repo" option).
2. On Render: **New +** → **Web Service** → connect the repo.
3. Settings:
   - **Environment**: Node
   - **Build command**: `npm install`
   - **Start command**: `npm start`
   - **Instance type**: Free is fine to start.
4. Once deployed, Render gives you a URL like `https://your-app.onrender.com`.
   The chat app connects over WebSocket, so use
   `wss://your-app.onrender.com` (note `wss://`, not `https://`).

## Run locally (for testing)

```bash
npm install
npm start
```
Server listens on `http://localhost:3000` (WebSocket at `ws://localhost:3000`).

## Notes

- Free Render web services "sleep" after inactivity — the first connection
  after a quiet period may take several seconds to wake up.
- `data.json` lives on the service's local disk. On Render's free tier this
  disk is **not guaranteed to persist across deploys** (it does survive
  restarts/sleep, just not necessarily a fresh `git push` deploy). For
  guaranteed durable storage, attach a Render persistent disk (paid) or
  swap `data.json` for a small external database later — the code only
  touches storage through `loadDB()` / `saveDB()`, so that's a contained change.
