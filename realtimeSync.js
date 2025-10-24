// --- realtimeSync.js ---
import 'dotenv/config';
import fs from 'fs';
import http from 'node:http';
import { MongoClient } from 'mongodb';
import { google } from 'googleapis';
import fetch from 'node-fetch';
import { COLUMNS, docToRow } from './fieldMap.js';

// Decode Google credentials
if (process.env.GOOGLE_CREDENTIALS_BASE64) {
  fs.writeFileSync(
    './credentials.json',
    Buffer.from(process.env.GOOGLE_CREDENTIALS_BASE64, 'base64').toString('utf8')
  );
}

// Env variables
const {
  SPREADSHEET_ID,
  SHEET_NAME = 'MongoSheet',
  MONGO_URI,
  MONGO_DB,
  MONGO_COLLECTION,
  INTERAKT_API_KEY,
  PORT = 3000,
} = process.env;

// Google Sheets setup
const credentials = JSON.parse(fs.readFileSync('./credentials.json', 'utf-8'));
const auth = new google.auth.GoogleAuth({
  credentials,
  scopes: [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
  ],
});
const sheets = google.sheets({ version: 'v4', auth });

// MongoDB setup
let client;
let col;
const idToRow = new Map();

/* ────────────── Helper: Range Builder ────────────── */
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

/* ────────────── Helper: Build Sheet Index ────────────── */
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

/* ────────────── Helper: Retry Wrapper ────────────── */
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

/* ────────────── Helper: Normalize Phone ────────────── */
function normalizePhone(raw, defaultCC = '+91') {
  if (!raw) return null;
  const digits = String(raw).replace(/[^\d+]/g, '');
  if (digits.startsWith('+')) return digits;
  const noLeadZero = digits.replace(/^0+/, '');
  return `${defaultCC}${noLeadZero}`;
}

/* ────────────── Sync to Interakt ────────────── */
async function syncToInterakt(doc) {
  // Normalize phone for Interakt formatting (still keeps +91 if missing)
  const raw = String(doc.mobile || '').trim();
  const normalized = raw.startsWith('+') ? raw : `+91${raw.replace(/^0+/, '')}`;

  // Build unique ID (even if phone duplicates exist)
  const uniqueId = `${doc._id}_${Math.floor(Math.random() * 100000)}`;

  const payload = {
    phoneNumber: normalized || '+911111111111', // fallback number (Interakt requires something)
    userId: uniqueId, // always unique, no overwrite
    traits: {
      name: doc.fullName || 'Unnamed',
      email: doc.email || undefined,
      grade: doc.grade || '',
      subject: doc.subject || '',
      pipeline_stage: 'New Lead',
      lead_source: 'Google Sheet',
    },
    tags: ['GoogleSheet'],
  };

  return await withRetry(async () => {
    const res = await fetch('https://api.interakt.ai/v1/public/track/users/', {
      method: 'POST',
      headers: {
        'Authorization': `Basic ${INTERAKT_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const text = await res.text();
      console.error(`❌ Failed to sync ${doc.fullName || 'Unnamed'}: ${res.status} ${res.statusText}`);
      console.error(`Payload:`, payload);
      console.error(`Response:`, text);
      throw new Error(`Interakt rejected ${doc.fullName || doc._id}`);
    }

    console.log(`✅ Synced to Interakt: ${doc.fullName || 'Unnamed'} (${normalized})`);
  }, 'syncToInterakt');
}

/* ────────────── Google Sheets CRUD ────────────── */
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
  await syncToInterakt(doc);
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
  await syncToInterakt(doc);
}

async function clearRow(rowNumber) {
  await withRetry(() =>
    sheets.spreadsheets.values.clear({
      spreadsheetId: SPREADSHEET_ID,
      range: rowRangeA1(rowNumber),
    }), 'clearRow');
}

/* ────────────── Watch MongoDB Changes ────────────── */
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
          console.log('ℹ️ Update no index → append:', id);
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

/* ────────────── Main Sync Start ────────────── */
async function startSync() {
  try {
    if (client) await client.close().catch(() => {});
    client = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 5000 });
    await client.connect();
    col = client.db(MONGO_DB).collection(MONGO_COLLECTION);

    const docs = await col.find({}).toArray();
    const header = COLUMNS.map((c) => c.header);
    const rows = docs.map(docToRow);
    const values = [header, ...rows];

    await withRetry(() =>
      sheets.spreadsheets.values.clear({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A:ZZ`,
      }), 'clearSheet'
    );

    await withRetry(() =>
      sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A1`,
        valueInputOption: 'RAW',
        requestBody: { values },
      }), 'initialExport'
    );

    console.log(`✅ Exported ${docs.length} documents to Google Sheet`);

    let sent = 0;

    for (const doc of docs) {
      await syncToInterakt(doc);
      sent++;
    }

    console.log(`📊 Interakt sync summary → Sent: ${sent} (All rows pushed to Interakt, no filters applied)`);

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

/* ────────────── HTTP Keepalive ────────────── */
http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('✅ MongoDB → Google Sheets & Interakt sync is running');
}).listen(PORT, () => {
  console.log(`🌍 Server running on port ${PORT}`);
});