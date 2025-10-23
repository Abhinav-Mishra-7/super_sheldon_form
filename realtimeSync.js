import 'dotenv/config';
import fs from 'fs';
import http from 'node:http';
import { MongoClient } from 'mongodb';
import { google } from 'googleapis';
import { COLUMNS, docToRow } from './fieldMap.js';

/* ───────────── 1️⃣ Decode credentials if Base64 provided ───────────── */
if (process.env.GOOGLE_CREDENTIALS_BASE64) {
  fs.writeFileSync(
    './credentials.json',
    Buffer.from(process.env.GOOGLE_CREDENTIALS_BASE64, 'base64').toString('utf8')
  );
}

/* ───────────── 2️⃣ Environment Variables ───────────── */
const {
  SPREADSHEET_ID,
  SHEET_NAME = 'MongoSheet',
  MONGO_URI,
  MONGO_DB,
  MONGO_COLLECTION,
  PORT = 3000,
} = process.env;

/* ───────────── 3️⃣ Google Sheets Setup ───────────── */
const credentials = JSON.parse(fs.readFileSync('./credentials.json', 'utf-8'));
const auth = new google.auth.GoogleAuth({
  credentials,
  scopes: [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
  ],
});
const sheets = google.sheets({ version: 'v4', auth });

/* ───────────── 4️⃣ MongoDB Setup ───────────── */
let client;
let col;
const idToRow = new Map(); // Maps _id → Row Number

function rowRangeA1(rowNumber) {
  const toCol = (n) => {
    let s = '';
    while (n > 0) {
      const r = (n - 1) % 26;
      s = String.fromCharCode(65 + r) + s;
      n = Math.floor((n - 1) / 26);
    }
    return s;
  };
  return `${SHEET_NAME}!A${rowNumber}:${toCol(COLUMNS.length)}${rowNumber}`;
}

/* ───────────── 5️⃣ Index existing sheet ───────────── */
async function buildIndexFromSheet() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!A2:ZZ`,
  });
  const rows = res.data.values ?? [];
  const idCol = COLUMNS.findIndex((c) => c.key === '_id');
  let rowNum = 2;
  idToRow.clear();
  for (const row of rows) {
    const idCell = row[idCol] ?? '';
    if (idCell) idToRow.set(idCell, rowNum);
    rowNum++;
  }
  console.log(`🔎 Indexed ${idToRow.size} rows from sheet`);
}

/* ───────────── 6️⃣ Retry Helpers ───────────── */
async function withRetry(fn, label = 'operation', retries = 3, delay = 1000) {
  try {
    return await fn();
  } catch (err) {
    if (retries <= 0) {
      console.error(`❌ ${label} failed after retries:`, err.message);
      return;
    }
    console.warn(`⚠️ ${label} failed: ${err.message}. Retrying in ${delay}ms...`);
    await new Promise((res) => setTimeout(res, delay));
    return withRetry(fn, label, retries - 1, delay * 2);
  }
}

/* ───────────── 7️⃣ CRUD Helpers ───────────── */
async function appendRow(doc) {
  const row = docToRow(doc);
  await withRetry(async () => {
    const res = await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!A:A`,
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [row] },
    });
    const updated = res.data.updates?.updatedRange;
    const m = updated?.match(/![A-Z]+(\d+):/);
    const rowNumber = m ? parseInt(m[1], 10) : null;
    if (rowNumber) idToRow.set(String(doc._id), rowNumber);
  }, 'appendRow');
}

async function updateRow(rowNumber, doc) {
  const row = docToRow(doc);
  await withRetry(() =>
    sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: rowRangeA1(rowNumber),
      valueInputOption: 'RAW',
      requestBody: { values: [row] },
    }), 'updateRow');
}

async function clearRow(rowNumber) {
  await withRetry(() =>
    sheets.spreadsheets.values.clear({
      spreadsheetId: SPREADSHEET_ID,
      range: rowRangeA1(rowNumber),
    }), 'clearRow');
}

/* ───────────── 8️⃣ Watch MongoDB with Auto-Reconnect ───────────── */
async function watchChanges() {
  console.log('👂 Watching MongoDB changes...');
  const stream = col.watch([], { fullDocument: 'updateLookup' });

  stream.on('change', async (change) => {
    try {
      const id = String(change.documentKey._id);
      if (change.operationType === 'insert') {
        console.log('➕ Insert:', id);
        await appendRow(change.fullDocument);
      } else if (['update', 'replace'].includes(change.operationType)) {
        const rowNumber = idToRow.get(id);
        if (rowNumber) {
          console.log('✏️ Update:', id, '→ row', rowNumber);
          await updateRow(rowNumber, change.fullDocument);
        } else {
          console.log('ℹ️ Update without index → append:', id);
          await appendRow(change.fullDocument);
        }
      } else if (change.operationType === 'delete') {
        const rowNumber = idToRow.get(id);
        if (rowNumber) {
          console.log('🗑 Delete:', id, 'row', rowNumber);
          await clearRow(rowNumber);
          idToRow.delete(id);
        }
      }
    } catch (e) {
      console.error('❌ Handler error:', e.message);
    }
  });

  stream.on('error', (e) => {
    console.error('⚠️ Change stream error:', e.message);
    console.log('🔄 Reconnecting in 10s...');
    setTimeout(startSync, 10000);
  });

  stream.on('close', () => {
    console.warn('⚠️ Change stream closed. Reconnecting in 10s...');
    setTimeout(startSync, 10000);
  });
}

/* ───────────── 9️⃣ Main Sync Startup ───────────── */
async function startSync() {
  try {
    if (client) await client.close().catch(() => {});
    client = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 5000 });
    await client.connect();
    col = client.db(MONGO_DB).collection(MONGO_COLLECTION);

    // Set headers
    await withRetry(() =>
      sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A1`,
        valueInputOption: 'RAW',
        requestBody: { values: [COLUMNS.map((c) => c.header)] },
      }), 'setHeaders');

    await buildIndexFromSheet();
    await watchChanges();

    console.log('✅ MongoDB connected and watching.');
  } catch (err) {
    console.error('❌ Sync start error:', err.message);
    console.log('🔁 Retrying in 15 seconds...');
    setTimeout(startSync, 15000);
  }
}

startSync();

/* 🔟 Keep Service Alive (for Render or Railway) */
http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('✅ MongoDB → Google Sheets sync is running');
}).listen(PORT, () => {
  console.log(`🌍 HTTP server running on port ${PORT}`);
});
