import 'dotenv/config';
import fs from 'fs';
import { MongoClient } from 'mongodb';
import { google } from 'googleapis';
import { COLUMNS, docToRow } from './fieldMap.js';

if (process.env.GOOGLE_CREDENTIALS_BASE64) {
  fs.writeFileSync(
    "./credentials.json",
    Buffer.from(process.env.GOOGLE_CREDENTIALS_BASE64, "base64").toString("utf8")
  );
}


const {
  SPREADSHEET_ID,
  SHEET_NAME = "MongoSheet",
  MONGO_URI,
  MONGO_DB,
  MONGO_COLLECTION
} = process.env;

const credentials = JSON.parse(fs.readFileSync('./credentials.json', 'utf-8'));
const auth = new google.auth.GoogleAuth({
  credentials,
  scopes: ['https://www.googleapis.com/auth/spreadsheets']
});
const sheets = google.sheets({ version: 'v4', auth });
const client = new MongoClient(MONGO_URI);

async function main() {
  try {
    await client.connect();
    const col = client.db(MONGO_DB).collection(MONGO_COLLECTION);

    const docs = await col.find({}).toArray();

    const header = COLUMNS.map(c => c.header);
    const rows = docs.map(docToRow);
    const values = [header, ...rows];

    // optional: clear old data
    await sheets.spreadsheets.values.clear({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!A:ZZ`
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!A1`,
      valueInputOption: 'RAW',
      requestBody: { values }
    });

    console.log(`✅ Exported ${docs.length} rows to ${SHEET_NAME}`);
  } catch (e) {
    console.error('❌ Export failed:', e.message);
  } finally {
    await client.close();
  }
}

main();
