import 'dotenv/config';
import fs from 'fs';
import { MongoClient } from 'mongodb';
import { google } from 'googleapis';
import { COLUMNS, docToRow } from './fieldMap.js';

if (process.env.GOOGLE_CREDENTIALS_BASE64) {
  fs.writeFileSync(
    './credentials.json',
    Buffer.from(process.env.GOOGLE_CREDENTIALS_BASE64, 'base64').toString('utf8')
  );
}

const {
  SPREADSHEET_ID,
  SHEET_NAME = 'Sheet1',
  MONGO_URI,
  MONGO_DB,
  MONGO_COLLECTION,
} = process.env;

const credentials = JSON.parse(fs.readFileSync('./credentials.json', 'utf-8'));
const auth = new google.auth.GoogleAuth({
  credentials,
  scopes: [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
  ],
});
const sheets = google.sheets({ version: 'v4', auth });


const client = new MongoClient(MONGO_URI);
const idToRow = new Map(); 

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

async function buildIndexFromSheet() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!A2:ZZ`,
  });
  const rows = res.data.values ?? [];
  const idCol = COLUMNS.findIndex((c) => c.key === '_id');
  let rowNum = 2;
  for (const row of rows) {
    const idCell = row[idCol] ?? '';
    if (idCell) idToRow.set(idCell, rowNum);
    rowNum++;
  }
  console.log(`🔎 Indexed ${idToRow.size} rows from sheet`);
}

async function appendRow(doc) {
  const row = docToRow(doc);
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
}

async function updateRow(rowNumber, doc) {
  const row = docToRow(doc);
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: rowRangeA1(rowNumber),
    valueInputOption: 'RAW',
    requestBody: { values: [row] },
  });
}

async function clearRow(rowNumber) {
  await sheets.spreadsheets.values.clear({
    spreadsheetId: SPREADSHEET_ID,
    range: rowRangeA1(rowNumber),
  });
}

async function main() {
  await client.connect();
  const col = client.db(MONGO_DB).collection(MONGO_COLLECTION);

  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!A1`,
    valueInputOption: 'RAW',
    requestBody: { values: [COLUMNS.map((c) => c.header)] },
  });

  await buildIndexFromSheet();

  console.log('👂 Watching MongoDB changes…');
  const stream = col.watch([], { fullDocument: 'updateLookup' });

  stream.on('change', async (change) => {
    try {
      if (change.operationType === 'insert') {
        const doc = change.fullDocument;
        console.log('➕ insert', doc._id);
        await appendRow(doc);
      } else if (['update', 'replace'].includes(change.operationType)) {
        const doc = change.fullDocument;
        const id = String(doc._id);
        const rowNumber = idToRow.get(id);
        if (rowNumber) {
          console.log('✏️ update', id, '→ row', rowNumber);
          await updateRow(rowNumber, doc);
        } else {
          console.log('ℹ️ update (no index) → append', id);
          await appendRow(doc);
        }
      } else if (change.operationType === 'delete') {
        const id = String(change.documentKey._id);
        const rowNumber = idToRow.get(id);
        console.log('🗑 delete', id, 'row', rowNumber);
        if (rowNumber) {
          await clearRow(rowNumber);
          idToRow.delete(id);
        }
      }
    } catch (e) {
      console.error('Handler error:', e.message);
    }
  });

  stream.on('error', (e) => {
    console.error('Change stream error:', e);
    process.exit(1);
  });
}

main().catch((e) => {
  console.error('❌ Startup error:', e);
  process.exit(1);
});

http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('✅ Running MongoDB → Google Sheets Sync Service');
}).listen(process.env.PORT || 3000, () => {
  console.log(`🌍 HTTP server running on port ${process.env.PORT || 3000}`);
});