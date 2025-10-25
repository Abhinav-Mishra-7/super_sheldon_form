// --- realtimeSync.js ---
import 'dotenv/config';
import fs from 'fs';
import express from 'express';
import { MongoClient } from 'mongodb';
import { google } from 'googleapis';
import axios from 'axios';
import { COLUMNS, docToRow, AIRTABLE_COLUMNS, airtableRecordToRow } from './fieldMap.js';

// 🔹 Decode Google credentials
if (process.env.GOOGLE_CREDENTIALS_BASE64) {
  fs.writeFileSync(
    './credentials.json',
    Buffer.from(process.env.GOOGLE_CREDENTIALS_BASE64, 'base64').toString('utf8')
  );
}

// 🔹 Environment variables
const {
  SPREADSHEET_ID,
  SHEET_NAME = 'Leads',
  AIRTABLE_SHEET_NAME = 'Demo Booking Form',
  MONGO_URI,
  MONGO_DB,
  MONGO_COLLECTION,
  INTERAKT_API_KEY,
  PORT = 3000,
} = process.env;

// 🔹 Google Sheets setup
const credentials = JSON.parse(fs.readFileSync('./credentials.json', 'utf-8'));
const auth = new google.auth.GoogleAuth({
  credentials,
  scopes: [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
  ],
});
const sheets = google.sheets({ version: 'v4', auth });

// 🔹 MongoDB setup
let client;
let col;
const idToRow = new Map();

// ────────────────────────────── Helpers ────────────────────────────── //
function rowRangeA1(sheetName, rowNumber) {
  const toCol = (n) => {
    let s = '';
    while (n > 0) {
      const r = (n - 1) % 26;
      s = String.fromCharCode(65 + r) + s;
      n = Math.floor((n - 1) / 26);
    }
    return s;
  };
  return `${sheetName}!A${rowNumber}:${toCol(COLUMNS.length)}${rowNumber}`;
}

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
  console.log(`🔎 Indexed ${idToRow.size} rows from ${SHEET_NAME} sheet`);
}

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

// ────────────────────────────── Interakt Sync ────────────────────────────── //
async function syncToInterakt(recordData, source = 'GoogleSheet') {
  try {
    // Extract and validate phone number
    let phoneNumber = recordData.phoneNumber 
      || recordData['Student Contact Number']
      || recordData.contactNumber
      || recordData.phone
      || recordData.mobile
      || recordData.studentContactNumber
      || recordData['Phone Number'];

    // Skip if no phone number
    if (!phoneNumber) {
      console.log(`⚠️ Skipping Interakt sync (${source}): No phone number for ${recordData._id || recordData.recordId}`);
      return;
    }

    // Clean phone number
    phoneNumber = String(phoneNumber).replace(/[^\d+]/g, '');
    
    // Add country code if missing
    if (!phoneNumber.startsWith('+')) {
      let countryCode = recordData.countryCode 
        || recordData['Country Code']
        || '+91';
      
      if (!countryCode.startsWith('+')) {
        countryCode = `+${countryCode}`;
      }
      phoneNumber = `${countryCode}${phoneNumber}`;
    }

    // Validate phone number
    const digitCount = phoneNumber.replace(/\D/g, '').length;
    if (digitCount < 10) {
      console.log(`⚠️ Skipping Interakt sync (${source}): Invalid phone ${phoneNumber}`);
      return;
    }

    // Determine tag based on source
    const tag = source === 'Airtable' ? 'Demo Booking Form' : 'Leads';

    const payload = {
      userId: String(recordData._id || recordData.recordId || `${source}-${Date.now()}`),
      phoneNumber: phoneNumber,
      traits: {
        ...recordData,
        phoneNumber: phoneNumber,
        source: source,
      },
      add_to_sales_cycle: true,
      lead_status_crm: 'New Lead',
      tags: [tag],
    };

    await axios.post('https://api.interakt.ai/v1/public/track/users/', payload, {
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Basic ${INTERAKT_API_KEY}`,
      },
    });

    console.log(`✅ Synced ${source} record to Interakt [${tag}]: ${phoneNumber}`);
  } catch (err) {
    const recordId = recordData._id || recordData.recordId || 'unknown';
    console.error(`❌ Interakt sync error (${source} - ${recordId}):`, err.response?.data || err.message);
  }
}

// ────────────────────────────── Google Sheets CRUD ────────────────────────────── //
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
  await syncToInterakt(doc, 'GoogleSheet');
}

async function updateRow(rowNumber, doc) {
  const row = docToRow(doc);
  await withRetry(() =>
    sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: rowRangeA1(SHEET_NAME, rowNumber),
      valueInputOption: 'RAW',
      requestBody: { values: [row] },
    }), 'updateRow');
  await syncToInterakt(doc, 'GoogleSheet');
}

async function clearRow(rowNumber) {
  await withRetry(() =>
    sheets.spreadsheets.values.clear({
      spreadsheetId: SPREADSHEET_ID,
      range: rowRangeA1(SHEET_NAME, rowNumber),
    }), 'clearRow');
}

// ────────────────────────────── MongoDB Change Stream ────────────────────────────── //
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

// ────────────────────────────── MongoDB Start ────────────────────────────── //
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

    console.log(`✅ Exported ${docs.length} documents to ${SHEET_NAME} sheet`);

    for (const doc of docs) {
      await syncToInterakt(doc, 'GoogleSheet');
    }

    console.log('📊 Initial GoogleSheet Interakt sync done.');
    await buildIndexFromSheet();
    await watchChanges();
  } catch (err) {
    console.error('❌ Sync start error:', err.message);
    console.log('🔁 Retrying in 15 seconds...');
    setTimeout(startSync, 15000);
  }
}

startSync();

// ────────────────────────────── Express Server ────────────────────────────── //
const app = express();
app.use(express.json());

// Health check
app.get('/', (req, res) => {
  res.send('✅ MongoDB → Google Sheets [Leads] & Airtable [Demo Booking Form] → Interakt sync running');
});

// ────────────────────────────── Airtable Webhook ────────────────────────────── //
app.post('/airtable-webhook', async (req, res) => {
  try {
    const payload = req.body;
    console.log('📩 Airtable webhook event received');

    for (const event of payload.payloads || []) {
      const tableId = event.tableId || '';
      if (tableId !== 'tbltM2TJ4yDQOpbdW') continue;

      const newRecords = event.changedTables?.[0]?.createdRecords || [];

      for (const record of newRecords) {
        try {
          const recordFields = record.fields || {};
          
          // ✅ ALL Airtable fields preserved with original names
          const airtableRecord = {
            recordId: record.id,
            ...recordFields, // Everything from Airtable as-is
            createdAt: new Date().toISOString(),
          };

          console.log(`📋 Airtable Record: ${record.id}`);

          // Send to Interakt with original field names
          await syncToInteraktAirtable(airtableRecord);

          // Optionally: Save to Google Sheet if needed
          // await appendAirtableRowToSheet(airtableRecord);

          console.log(`✅ Processed: ${record.id}`);
        } catch (recordErr) {
          console.error(`❌ Error: ${record.id}:`, recordErr.message);
        }
      }
    }

    res.status(200).json({ success: true });
  } catch (err) {
    console.error('❌ Webhook error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});


app.listen(PORT, () => {
  console.log(`🌍 Server listening on port ${PORT}`);
});







// // --- realtimeSync.js ---
// import 'dotenv/config';
// import fs from 'fs';
// import http from 'node:http';
// import { MongoClient } from 'mongodb';
// import { google } from 'googleapis';
// import { COLUMNS, docToRow } from './fieldMap.js';
// import axios from 'axios';
// import express from "express";
// const app = express();
// app.use(express.json());


// // Decode Google credentials
// if (process.env.GOOGLE_CREDENTIALS_BASE64) {
//   fs.writeFileSync(
//     './credentials.json',
//     Buffer.from(process.env.GOOGLE_CREDENTIALS_BASE64, 'base64').toString('utf8')
//   );
// }

// // Env variables
// const {
//   SPREADSHEET_ID,
//   SHEET_NAME = 'MongoSheet',
//   MONGO_URI,
//   MONGO_DB,
//   MONGO_COLLECTION,
//   INTERAKT_API_KEY,
//   PORT = 3000,
// } = process.env;

// // Google Sheets setup
// const credentials = JSON.parse(fs.readFileSync('./credentials.json', 'utf-8'));
// const auth = new google.auth.GoogleAuth({
//   credentials,
//   scopes: [
//     'https://www.googleapis.com/auth/spreadsheets',
//     'https://www.googleapis.com/auth/drive',
//   ],
// });
// const sheets = google.sheets({ version: 'v4', auth });

// // MongoDB setup
// let client;
// let col;
// const idToRow = new Map();

// /* ────────────── Helper: Range Builder ────────────── */
// function rowRangeA1(rowNumber) {
//   const toCol = (n) => {
//     let s = '';
//     while (n > 0) {
//       const r = (n - 1) % 26;
//       s = String.fromCharCode(65 + r) + s;
//       n = Math.floor((n - 1) / 26);
//     }
//     return s;
//   };
//   return `${SHEET_NAME}!A${rowNumber}:${toCol(COLUMNS.length)}${rowNumber}`;
// }

// /* ────────────── Helper: Build Sheet Index ────────────── */
// async function buildIndexFromSheet() {
//   const res = await sheets.spreadsheets.values.get({
//     spreadsheetId: SPREADSHEET_ID,
//     range: `${SHEET_NAME}!A2:ZZ`,
//   });
//   const rows = res.data.values ?? [];
//   const idCol = COLUMNS.findIndex((c) => c.key === '_id');
//   let rowNum = 2;
//   idToRow.clear();
//   for (const row of rows) {
//     const idCell = row[idCol] ?? '';
//     if (idCell) idToRow.set(idCell, rowNum);
//     rowNum++;
//   }
//   console.log(`🔎 Indexed ${idToRow.size} rows from sheet`);
// }

// /* ────────────── Helper: Retry Wrapper ────────────── */
// async function withRetry(fn, label = 'operation', retries = 3, delay = 1000) {
//   try {
//     return await fn();
//   } catch (err) {
//     if (retries <= 0) {
//       console.error(`❌ ${label} failed after retries:`, err.message);
//       return;
//     }
//     console.warn(`⚠️ ${label} failed: ${err.message}. Retrying in ${delay}ms...`);
//     await new Promise((res) => setTimeout(res, delay));
//     return withRetry(fn, label, retries - 1, delay * 2);
//   }
// }

// /* ────────────── Helper: Normalize Phone ────────────── */
// function normalizePhone(raw, defaultCC = '+91') {
//   if (!raw) return null;
//   const digits = String(raw).replace(/[^\d+]/g, '');
//   if (digits.startsWith('+')) return digits;
//   const noLeadZero = digits.replace(/^0+/, '');
//   return `${defaultCC}${noLeadZero}`;
// }

// /* ────────────── Sync to Interakt ────────────── */
// async function syncToInterakt(doc) {
//   // const uniqueSuffix = `${Date.now()}_${Math.floor(Math.random() * 1000)}`;
//   const normalized = String(doc.mobile || '').trim() || '+911111111111';
//   const apiUrl = "https://api.interakt.ai/v1/public/track/users/";

//   const payload = {
//     phoneNumber: normalized,
//     userId: String(doc._id), 
//     traits: {
//       name: doc.fullName || '',
//       email: doc.email || '',
//       grade: doc.grade || '',
//       subject: doc.subject || ''    
//     },
//     add_to_sales_cycle: true,                   
//     lead_status_crm: "New Lead" ,
//     tags: ['GoogleSheet']
//   };


//  try {
//     const response = await axios.post(apiUrl, payload, {
//       headers: {
//         "Content-Type": "application/json",
//         "Authorization": `Basic ${INTERAKT_API_KEY}`  
//       }
//     });
//     console.log(`Synced contact ${doc.phone} to Interakt as a New Lead.`);
//   } catch (err) {
//     console.error(`Failed to sync contact ${doc.phone}:`, err);
//   }
// }

// /* ────────────── Google Sheets CRUD ────────────── */
// async function appendRow(doc) {
//   const row = docToRow(doc);
//   await withRetry(async () => {
//     const res = await sheets.spreadsheets.values.append({
//       spreadsheetId: SPREADSHEET_ID,
//       range: `${SHEET_NAME}!A:A`,
//       valueInputOption: 'RAW',
//       insertDataOption: 'INSERT_ROWS',
//       requestBody: { values: [row] },
//     });
//     const updated = res.data.updates?.updatedRange;
//     const m = updated?.match(/![A-Z]+(\d+):/);
//     const rowNumber = m ? parseInt(m[1], 10) : null;
//     if (rowNumber) idToRow.set(String(doc._id), rowNumber);
//   }, 'appendRow');
//   await syncToInterakt(doc);
// }

// async function updateRow(rowNumber, doc) {
//   const row = docToRow(doc);
//   await withRetry(() =>
//     sheets.spreadsheets.values.update({
//       spreadsheetId: SPREADSHEET_ID,
//       range: rowRangeA1(rowNumber),
//       valueInputOption: 'RAW',
//       requestBody: { values: [row] },
//     }), 'updateRow');
//   await syncToInterakt(doc);
// }

// async function clearRow(rowNumber) {
//   await withRetry(() =>
//     sheets.spreadsheets.values.clear({
//       spreadsheetId: SPREADSHEET_ID,
//       range: rowRangeA1(rowNumber),
//     }), 'clearRow');
// }

// /* ────────────── Watch MongoDB Changes ────────────── */
// async function watchChanges() {
//   console.log('👂 Watching MongoDB changes...');
//   const stream = col.watch([], { fullDocument: 'updateLookup' });

//   stream.on('change', async (change) => {
//     try {
//       const id = String(change.documentKey._id);
//       if (change.operationType === 'insert') {
//         console.log('➕ Insert:', id);
//         await appendRow(change.fullDocument);
//       } else if (['update', 'replace'].includes(change.operationType)) {
//         const rowNumber = idToRow.get(id);
//         if (rowNumber) {
//           console.log('✏️ Update:', id, '→ row', rowNumber);
//           await updateRow(rowNumber, change.fullDocument);
//         } else {
//           console.log('ℹ️ Update no index → append:', id);
//           await appendRow(change.fullDocument);
//         }
//       } else if (change.operationType === 'delete') {
//         const rowNumber = idToRow.get(id);
//         if (rowNumber) {
//           console.log('🗑 Delete:', id, 'row', rowNumber);
//           await clearRow(rowNumber);
//           idToRow.delete(id);
//         }
//       }
//     } catch (e) {
//       console.error('❌ Handler error:', e.message);
//     }
//   });

//   stream.on('error', (e) => {
//     console.error('⚠️ Change stream error:', e.message);
//     console.log('🔄 Reconnecting in 10s...');
//     setTimeout(startSync, 10000);
//   });

//   stream.on('close', () => {
//     console.warn('⚠️ Change stream closed. Reconnecting in 10s...');
//     setTimeout(startSync, 10000);
//   });
// }

// /* ────────────── Main Sync Start ────────────── */
// async function startSync() {
//   try {
//     if (client) await client.close().catch(() => {});
//     client = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 5000 });
//     await client.connect();
//     col = client.db(MONGO_DB).collection(MONGO_COLLECTION);

//     const docs = await col.find({}).toArray();
//     const header = COLUMNS.map((c) => c.header);
//     const rows = docs.map(docToRow);
//     const values = [header, ...rows];

//     await withRetry(() =>
//       sheets.spreadsheets.values.clear({
//         spreadsheetId: SPREADSHEET_ID,
//         range: `${SHEET_NAME}!A:ZZ`,
//       }), 'clearSheet'
//     );

//     await withRetry(() =>
//       sheets.spreadsheets.values.update({
//         spreadsheetId: SPREADSHEET_ID,
//         range: `${SHEET_NAME}!A1`,
//         valueInputOption: 'RAW',
//         requestBody: { values },
//       }), 'initialExport'
//     );

//     console.log(`✅ Exported ${docs.length} documents to Google Sheet`);

//     let sent = 0;

//     for (const doc of docs) {
//       await syncToInterakt(doc);
//       sent++;
//     }

//     console.log(`📊 Interakt sync summary → Sent: ${sent} (All rows pushed to Interakt, no filters applied)`);

//     await buildIndexFromSheet();
//     await watchChanges();

//     console.log('✅ MongoDB connected and watching.');
//   } catch (err) {
//     console.error('❌ Sync start error:', err.message);
//     console.log('🔁 Retrying in 15 seconds...');
//     setTimeout(startSync, 15000);
//   }
// }

// startSync();

// /* ────────────── HTTP Keepalive ────────────── */
// app.get("/", (req, res) => {
//   res.send("✅ MongoDB → Google Sheets & Airtable → Interakt sync running");
// });

// /* ────────────── Airtable Webhook Receiver ────────────── */
// app.post("/airtable-webhook", async (req, res) => {
//   try {
//     const payload = req.body;
//     console.log("📩 Airtable webhook event:", JSON.stringify(payload, null, 2));

//     // Handle all new record events (insert)
//     for (const event of payload.payloads || []) {
//       const tableId = event.tableId || "UnknownTable";
//       const newRecords = event.changedTables?.[0]?.createdRecords || [];

//       for (const record of newRecords) {
//         const recordData = record.fields || {};
//         const phone =
//           recordData.Phone ||
//           recordData.phone ||
//           Object.values(recordData).find((v) => /^\+?\d{10,}$/.test(v));

//         if (!phone) {
//           console.warn(`⚠️ No phone found in table ${tableId}`);
//           continue;
//         }

//         // Reuse your Interakt sync function (dynamic data)
//         const payload = {
//           phoneNumber: phone.startsWith("+") ? phone : `+91${phone}`,
//           userId: record.id,
//           traits: {
//             ...recordData,
//             tableName: tableId,
//             source: "Airtable",
//           },
//           add_to_sales_cycle: true,
//           lead_status_crm: "New Lead",
//           tags: [tableId],
//         };

//         await axios.post("https://api.interakt.ai/v1/public/track/users/", payload, {
//           headers: {
//             "Content-Type": "application/json",
//             Authorization: `Basic ${INTERAKT_API_KEY}`,
//           },
//         });

//         console.log(`✅ Synced record ${record.id} from table ${tableId}`);
//       }
//     }

//     res.status(200).send("ok");
//   } catch (err) {
//     console.error("❌ Airtable webhook error:", err.message);
//     res.status(500).send("error");
//   }
// });

// app.listen(PORT, () => {
//   console.log(`🌍 Server running on port ${PORT}`);
// });