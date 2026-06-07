process.env.TZ = 'Europe/Brussels';
require('dotenv').config();
const path = require('path');
const express = require('express');
const cookieSession = require('cookie-session');
const multer = require('multer');
const { stringify } = require('csv-stringify/sync');
const { createClient } = require('@supabase/supabase-js');

const app = express();
const PORT = process.env.PORT || 3000;
const publicDir = path.join(__dirname, 'public');

const SUPABASE_URL = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_PUBLISHABLE_KEY =
  process.env.SUPABASE_PUBLISHABLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_PUBLISHABLE_DEFAULT_KEY;
const SUPABASE_SECRET_KEY = process.env.SUPABASE_SECRET_KEY;
const SESSION_SECRET = process.env.SESSION_SECRET || 'interglobe-session-secret-change-me';
const PROOF_BUCKET = 'proof-photos';
const DEFAULT_GPS_STALE_MINUTES = Number(process.env.GPS_STALE_MINUTES || 30);
const STOP_OVERDUE_MINUTES = Number(process.env.STOP_OVERDUE_MINUTES || 90);

if (!SUPABASE_URL || !SUPABASE_PUBLISHABLE_KEY || !SUPABASE_SECRET_KEY) {
  console.warn(
    'Variables Supabase manquantes. Ajoute SUPABASE_URL, SUPABASE_PUBLISHABLE_KEY et SUPABASE_SECRET_KEY.'
  );
}

const admin = createClient(
  SUPABASE_URL || 'https://example.supabase.co',
  SUPABASE_SECRET_KEY || 'missing-secret-key',
  {
    auth: { autoRefreshToken: false, persistSession: false }
  }
);

function createAuthClient() {
  return createClient(
    SUPABASE_URL || 'https://example.supabase.co',
    SUPABASE_PUBLISHABLE_KEY || 'missing-publishable-key',
    {
      auth: { autoRefreshToken: false, persistSession: false }
    }
  );
}

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

app.use('/public', express.static(publicDir));
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(
  cookieSession({
    name: 'interglobe_session',
    secret: SESSION_SECRET,
    maxAge: 1000 * 60 * 60 * 8,
    sameSite: 'lax',
    httpOnly: true
  })
);

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024 }
});

function today() {
  const now = new Date();
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Brussels',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).format(now);
}

function nowIso() {
  return new Date().toISOString();
}

function nowBrusselsDate() {
  return new Date(new Date().toLocaleString('en-US', { timeZone: 'Europe/Brussels' }));
}

function flash(req, message) {
  req.session.flash = message;
}

function formatDateTime(value) {
  if (!value) return '';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;

  return d.toLocaleString('fr-BE', {
    timeZone: 'Europe/Brussels',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
  });
}

function sanitizeFileName(name = 'photo.jpg') {
  return name.replace(/[^a-zA-Z0-9._-]/g, '_');
}

function buildAddress(stop) {
  if (stop.full_address) return stop.full_address;
  return [
    [stop.street, stop.street_number].filter(Boolean).join(' '),
    [stop.postal_code, stop.city].filter(Boolean).join(' ')
  ]
    .filter(Boolean)
    .join(', ');
}

function mapWeekdaysInput(value) {
  if (!value) return [];
  const lookup = { sun: 0, mon: 1, tue: 2, wed: 3, thu: 4, fri: 5, sat: 6 };
  return String(value)
    .split(',')
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean)
    .map((item) => lookup[item])
    .filter((item) => Number.isInteger(item));
}

function weekdaysToInput(arr = []) {
  const lookup = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];
  return (arr || [])
    .map((n) => lookup[Number(n)])
    .filter(Boolean)
    .join(',');
}

function addDays(dateStr, days) {
  const d = new Date(`${dateStr}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

function dateRange(startDate, endDate) {
  const out = [];
  let cursor = startDate;
  while (cursor <= endDate) {
    out.push(cursor);
    cursor = addDays(cursor, 1);
  }
  return out;
}

function haversineKm(lat1, lon1, lat2, lon2) {
  const toRad = (v) => (v * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

function diffMinutesFromNow(isoDate) {
  if (!isoDate) return null;
  const value = new Date(isoDate);
  if (Number.isNaN(value.getTime())) return null;
  return Math.round((nowBrusselsDate().getTime() - value.getTime()) / 60000);
}

function ensureSupabaseConfigured(req, res, next) {
  if (!SUPABASE_URL || !SUPABASE_PUBLISHABLE_KEY || !SUPABASE_SECRET_KEY) {
    return res.status(500).send('Configuration Supabase incomplète.');
  }
  next();
}

async function fetchProfilesMap() {
  const { data, error } = await admin.from('profiles').select('*');
  if (error) throw error;
  return (data || []).reduce((acc, item) => {
    acc[item.id] = item;
    return acc;
  }, {});
}

async function fetchAllProfiles() {
  const { data, error } = await admin
    .from('profiles')
    .select('*')
    .order('created_at', { ascending: true });
  if (error) throw error;
  return data || [];
}

async function fetchProfileById(profileId) {
  const { data, error } = await admin
    .from('profiles')
    .select('*')
    .eq('id', profileId)
    .maybeSingle();
  if (error) throw error;
  return data;
}

function parseNumberOrNull(value) {
  if (value === undefined || value === null || value === '') return null;
  const normalized = String(value).replace(',', '.').trim();
  const number = Number(normalized);
  return Number.isFinite(number) ? number : null;
}

function formatMoney(value) {
  const number = Number(value || 0);
  return new Intl.NumberFormat('fr-BE', {
    style: 'currency',
    currency: 'EUR'
  }).format(number);
}


function loadPdfKit() {
  try {
    return require('pdfkit');
  } catch (error) {
    const missing = new Error('PDFKit non installé. Lance: npm install pdfkit');
    missing.code = 'PDFKIT_MISSING';
    throw missing;
  }
}

function pdfMoney(value) {
  return `${Number(value || 0).toFixed(2)} EUR`;
}

function safePdfText(value) {
  return String(value ?? '')
    .replace(/€/g, 'EUR')
    .replace(/[–—]/g, '-')
    .replace(/[“”]/g, '"')
    .replace(/[‘’]/g, "'");
}

function setupPdfResponse(res, filename) {
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
}

function addPdfHeader(doc, title, subtitle) {
  doc.font('Helvetica-Bold').fontSize(18).text(safePdfText(title));
  if (subtitle) {
    doc.moveDown(0.25);
    doc.font('Helvetica').fontSize(10).fillColor('#555555').text(safePdfText(subtitle));
    doc.fillColor('#000000');
  }
  doc.moveDown(1);
}

function addPdfKeyValue(doc, label, value, x, y, width = 240) {
  doc.font('Helvetica').fontSize(9).fillColor('#666666').text(safePdfText(label), x, y, { width });
  doc.font('Helvetica-Bold').fontSize(12).fillColor('#000000').text(safePdfText(value), x, y + 13, { width });
}

function ensurePdfSpace(doc, needed = 80) {
  if (doc.y + needed > doc.page.height - doc.page.margins.bottom) {
    doc.addPage();
  }
}

function addPdfTableHeader(doc, columns) {
  ensurePdfSpace(doc, 45);
  const startY = doc.y;
  doc.rect(doc.page.margins.left, startY - 4, doc.page.width - doc.page.margins.left - doc.page.margins.right, 20).fill('#eeeeee');
  doc.fillColor('#000000').font('Helvetica-Bold').fontSize(8);
  columns.forEach((col) => {
    doc.text(safePdfText(col.label), col.x, startY, { width: col.width, align: col.align || 'left' });
  });
  doc.y = startY + 24;
}

function addPdfTableRow(doc, columns, values) {
  ensurePdfSpace(doc, 34);
  const startY = doc.y;
  doc.font('Helvetica').fontSize(8).fillColor('#000000');
  columns.forEach((col) => {
    doc.text(safePdfText(values[col.key] ?? ''), col.x, startY, {
      width: col.width,
      align: col.align || 'left'
    });
  });
  doc.moveTo(doc.page.margins.left, startY + 20)
    .lineTo(doc.page.width - doc.page.margins.right, startY + 20)
    .strokeColor('#dddddd')
    .stroke();
  doc.strokeColor('#000000');
  doc.y = startY + 26;
}

function finishPdf(doc) {
  const range = doc.bufferedPageRange();
  for (let i = range.start; i < range.start + range.count; i += 1) {
    doc.switchToPage(i);
    doc.font('Helvetica').fontSize(8).fillColor('#777777')
      .text(`Page ${i + 1} / ${range.count}`, doc.page.margins.left, doc.page.height - 35, {
        width: doc.page.width - doc.page.margins.left - doc.page.margins.right,
        align: 'right'
      });
  }
  doc.end();
}

function buildDriverMonthlyRecapPdf(res, profile, recap) {
  const PDFDocument = loadPdfKit();
  const filename = `recap-${safePdfText(profile.username || profile.full_name || 'chauffeur')}-${recap.month}.pdf`.replace(/[^a-zA-Z0-9._-]/g, '_');
  setupPdfResponse(res, filename);

  const doc = new PDFDocument({ size: 'A4', margin: 36, bufferPages: true });
  doc.pipe(res);

  addPdfHeader(doc, 'Récapitulatif mensuel sous-traitant', `Mois: ${recap.month} - Chauffeur: ${profile.full_name || profile.username}`);

  const y = doc.y;
  addPdfKeyValue(doc, 'Courses du mois', String(recap.totals.allCount), 36, y);
  addPdfKeyValue(doc, 'Courses terminées', String(recap.totals.completedCount), 190, y);
  addPdfKeyValue(doc, 'Montant attentes', pdfMoney(recap.totals.totalWaitingAmount), 344, y);
  doc.y = y + 45;
  addPdfKeyValue(doc, 'Total à facturer - courses terminées', pdfMoney(recap.totals.totalCompletedAmount), 36, doc.y, 400);
  doc.moveDown(2);

  const columns = [
    { key: 'date', label: 'Date', x: 36, width: 55 },
    { key: 'reference', label: 'Référence', x: 94, width: 78 },
    { key: 'client', label: 'Client', x: 175, width: 80 },
    { key: 'status', label: 'Statut', x: 258, width: 56 },
    { key: 'price', label: 'Prix', x: 318, width: 60, align: 'right' },
    { key: 'waiting', label: 'Attente', x: 382, width: 60, align: 'right' },
    { key: 'total', label: 'Total', x: 446, width: 70, align: 'right' }
  ];
  addPdfTableHeader(doc, columns);

  recap.tours.forEach((tour) => {
    addPdfTableRow(doc, columns, {
      date: tour.date,
      reference: tour.reference,
      client: tour.client_name || '-',
      status: tour.status,
      price: pdfMoney(tour.subcontractor_price),
      waiting: pdfMoney(tour.waiting_amount),
      total: tour.status === 'completed' ? pdfMoney(tour.subcontractor_total) : '-'
    });
  });

  doc.moveDown(1);
  doc.font('Helvetica-Bold').fontSize(11).text(`Total à facturer: ${pdfMoney(recap.totals.totalCompletedAmount)}`, { align: 'right' });
  doc.font('Helvetica').fontSize(8).fillColor('#555555').text('Le total additionne uniquement les courses terminées: prix convenu + montant d’attente.', { align: 'right' });

  finishPdf(doc);
}

function buildDispatchSubcontractorRecapPdf(res, recap) {
  const PDFDocument = loadPdfKit();
  const filename = `recap-sous-traitants-${recap.month}.pdf`;
  setupPdfResponse(res, filename);

  const doc = new PDFDocument({ size: 'A4', margin: 36, bufferPages: true });
  doc.pipe(res);

  addPdfHeader(doc, 'Récapitulatif sous-traitants', `Mois: ${recap.month} - Vue dispatch`);

  const y = doc.y;
  addPdfKeyValue(doc, 'Courses du mois', String(recap.totals.allCount), 36, y);
  addPdfKeyValue(doc, 'Courses terminées', String(recap.totals.completedCount), 180, y);
  addPdfKeyValue(doc, 'Total attentes', pdfMoney(recap.totals.totalWaitingAmount), 324, y);
  doc.y = y + 45;
  addPdfKeyValue(doc, 'Total à payer', pdfMoney(recap.totals.totalAmount), 36, doc.y, 250);
  doc.moveDown(2);

  const columns = [
    { key: 'date', label: 'Date', x: 36, width: 52 },
    { key: 'reference', label: 'Référence', x: 90, width: 74 },
    { key: 'client', label: 'Client', x: 166, width: 72 },
    { key: 'status', label: 'Statut', x: 240, width: 52 },
    { key: 'price', label: 'Prix', x: 296, width: 58, align: 'right' },
    { key: 'waiting', label: 'Attente', x: 358, width: 58, align: 'right' },
    { key: 'total', label: 'Total', x: 420, width: 75, align: 'right' }
  ];

  recap.recaps.forEach((item, index) => {
    if (index > 0) doc.addPage();
    ensurePdfSpace(doc, 80);
    doc.font('Helvetica-Bold').fontSize(13).fillColor('#000000').text(safePdfText(item.driver.full_name || item.driver.username));
    doc.font('Helvetica').fontSize(9).fillColor('#555555').text(
      safePdfText(`${item.totals.completedCount} terminée(s) - ${item.totals.pendingCount} en attente - Attentes: ${pdfMoney(item.totals.totalWaitingAmount)} - Total: ${pdfMoney(item.totals.totalAmount)}`)
    );
    doc.fillColor('#000000').moveDown(0.75);

    addPdfTableHeader(doc, columns);
    if (!item.tours.length) {
      doc.font('Helvetica').fontSize(9).text('Aucune course pour ce mois.');
      doc.moveDown(1);
    }

    item.tours.forEach((tour) => {
      addPdfTableRow(doc, columns, {
        date: tour.date,
        reference: tour.reference,
        client: tour.client_name || '-',
        status: tour.status,
        price: pdfMoney(tour.subcontractor_price),
        waiting: pdfMoney(tour.waiting_amount),
        total: tour.status === 'completed' ? pdfMoney(tour.subcontractor_total) : '-'
      });
    });

    doc.moveDown(0.5);
    doc.font('Helvetica-Bold').fontSize(10).text(`Total ${item.driver.full_name || item.driver.username}: ${pdfMoney(item.totals.totalAmount)}`, { align: 'right' });
  });

  finishPdf(doc);
}


function currentMonthInput() {
  const value = today();
  return value.slice(0, 7);
}

function normalizeMonthInput(value) {
  const raw = String(value || '').trim();
  if (/^\d{4}-\d{2}$/.test(raw)) return raw;
  return currentMonthInput();
}

function monthBounds(monthValue) {
  const month = normalizeMonthInput(monthValue);
  const startDate = `${month}-01`;
  const d = new Date(`${startDate}T00:00:00Z`);
  d.setUTCMonth(d.getUTCMonth() + 1);
  const endDateExclusive = d.toISOString().slice(0, 10);
  return { month, startDate, endDateExclusive };
}

async function fetchDriverMonthlyRecap(driverProfileId, monthValue) {
  const { month, startDate, endDateExclusive } = monthBounds(monthValue);

  const { data: rows, error } = await admin
    .from('tours')
    .select('*')
    .eq('assigned_driver_profile_id', driverProfileId)
    .eq('driver_type', 'subcontractor')
    .gte('tour_date', startDate)
    .lt('tour_date', endDateExclusive)
    .order('tour_date', { ascending: true })
    .order('created_at', { ascending: true });
  if (error) throw error;

  const profilesMap = await fetchProfilesMap();
  const vehiclesMap = await fetchVehiclesMap();
  const tours = (rows || []).map((row) => normalizeTour(row, profilesMap, vehiclesMap));
  const completedTours = tours.filter((tour) => tour.status === 'completed');
  const pendingTours = tours.filter((tour) => tour.status !== 'completed');
  const totalCompletedAmount = completedTours.reduce(
    (sum, tour) => sum + Number(tour.subcontractor_total || 0),
    0
  );
  const totalPlannedAmount = tours.reduce((sum, tour) => sum + Number(tour.subcontractor_total || 0), 0);
  const totalWaitingAmount = completedTours.reduce((sum, tour) => sum + Number(tour.waiting_amount || 0), 0);

  return {
    month,
    startDate,
    endDateExclusive,
    tours,
    completedTours,
    pendingTours,
    totals: {
      allCount: tours.length,
      completedCount: completedTours.length,
      pendingCount: pendingTours.length,
      totalCompletedAmount,
      totalCompletedAmountLabel: formatMoney(totalCompletedAmount),
      totalWaitingAmount,
      totalWaitingAmountLabel: formatMoney(totalWaitingAmount),
      totalPlannedAmount,
      totalPlannedAmountLabel: formatMoney(totalPlannedAmount)
    }
  };
}


async function fetchSubcontractorMonthlyRecaps(monthValue) {
  const { month, startDate, endDateExclusive } = monthBounds(monthValue);

  const profiles = await fetchAllProfiles();
  const subcontractors = profiles.filter(
    (profile) => profile.role === 'driver' && profile.driver_type === 'subcontractor'
  );

  const { data: rows, error } = await admin
    .from('tours')
    .select('*')
    .eq('driver_type', 'subcontractor')
    .gte('tour_date', startDate)
    .lt('tour_date', endDateExclusive)
    .order('tour_date', { ascending: true })
    .order('created_at', { ascending: true });
  if (error) throw error;

  const profilesMap = profiles.reduce((acc, item) => {
    acc[item.id] = item;
    return acc;
  }, {});
  const vehiclesMap = await fetchVehiclesMap();
  const tours = (rows || []).map((row) => normalizeTour(row, profilesMap, vehiclesMap));

  const recaps = subcontractors.map((driver) => {
    const driverTours = tours.filter((tour) => tour.driver_id === driver.id);
    const completedTours = driverTours.filter((tour) => tour.status === 'completed');
    const pendingTours = driverTours.filter((tour) => tour.status !== 'completed');
    const totalBaseAmount = completedTours.reduce((sum, tour) => sum + Number(tour.subcontractor_price || 0), 0);
    const totalWaitingAmount = completedTours.reduce((sum, tour) => sum + Number(tour.waiting_amount || 0), 0);
    const totalAmount = totalBaseAmount + totalWaitingAmount;

    return {
      driver,
      tours: driverTours,
      completedTours,
      pendingTours,
      totals: {
        allCount: driverTours.length,
        completedCount: completedTours.length,
        pendingCount: pendingTours.length,
        totalBaseAmount,
        totalBaseAmountLabel: formatMoney(totalBaseAmount),
        totalWaitingAmount,
        totalWaitingAmountLabel: formatMoney(totalWaitingAmount),
        totalAmount,
        totalAmountLabel: formatMoney(totalAmount)
      }
    };
  });

  const globalTotals = recaps.reduce(
    (acc, recap) => {
      acc.allCount += recap.totals.allCount;
      acc.completedCount += recap.totals.completedCount;
      acc.pendingCount += recap.totals.pendingCount;
      acc.totalBaseAmount += recap.totals.totalBaseAmount;
      acc.totalWaitingAmount += recap.totals.totalWaitingAmount;
      acc.totalAmount += recap.totals.totalAmount;
      return acc;
    },
    { allCount: 0, completedCount: 0, pendingCount: 0, totalBaseAmount: 0, totalWaitingAmount: 0, totalAmount: 0 }
  );

  return {
    month,
    startDate,
    endDateExclusive,
    recaps,
    tours,
    totals: {
      ...globalTotals,
      totalBaseAmountLabel: formatMoney(globalTotals.totalBaseAmount),
      totalWaitingAmountLabel: formatMoney(globalTotals.totalWaitingAmount),
      totalAmountLabel: formatMoney(globalTotals.totalAmount)
    }
  };
}

function normalizeVehicle(row) {
  if (!row) return null;
  const length = Number(row.length_m || 0);
  const width = Number(row.width_m || 0);
  const height = Number(row.height_m || 0);
  const volume = length && width && height ? Number((length * width * height).toFixed(2)) : null;

  return {
    ...row,
    label: [row.plate, row.brand, row.model].filter(Boolean).join(' · '),
    dimensions_label:
      length || width || height
        ? `L ${length || '-'} m × l ${width || '-'} m × H ${height || '-'} m`
        : '-',
    volume_m3: volume,
    mma_label: row.mma_kg ? `${row.mma_kg} kg` : '-'
  };
}

async function fetchAllVehicles() {
  const { data, error } = await admin
    .from('vehicles')
    .select('*')
    .order('plate', { ascending: true });

  if (error) {
    console.warn('[VEHICLES] table indisponible ou requête impossible:', error.message);
    return [];
  }

  return (data || []).map(normalizeVehicle);
}

async function fetchVehiclesMap() {
  const vehicles = await fetchAllVehicles();
  return vehicles.reduce((acc, item) => {
    acc[item.id] = item;
    return acc;
  }, {});
}

async function getDriverAssignmentInfo(driverId) {
  if (!driverId) {
    return { driver_type: 'internal', vehicle_id: null, subcontractor_price: 0 };
  }

  const { data: driver, error } = await admin
    .from('profiles')
    .select('id, driver_type, vehicle_id')
    .eq('id', driverId)
    .maybeSingle();
  if (error) throw error;

  return {
    driver_type: driver?.driver_type === 'subcontractor' ? 'subcontractor' : 'internal',
    vehicle_id: driver?.vehicle_id || null
  };
}

async function getProfileByUsername(username) {
  const { data, error } = await admin
    .from('profiles')
    .select('*')
    .ilike('username', username)
    .maybeSingle();
  if (error) throw error;
  return data;
}

function normalizeTour(row, profilesMap, vehiclesMap = {}) {
  const driver = profilesMap[row.assigned_driver_profile_id] || null;
  const client = profilesMap[row.client_profile_id] || null;
  const vehicle = vehiclesMap[row.vehicle_id || driver?.vehicle_id] || null;
  const driverType = row.driver_type || driver?.driver_type || 'internal';
  const subcontractorPrice = Number(row.subcontractor_price || 0);
  const waitingAmount = Number(row.waiting_amount || 0);
  const subcontractorTotal = subcontractorPrice + waitingAmount;

  return {
    ...row,
    reference: row.reference_code,
    date: row.tour_date,
    driver_id: row.assigned_driver_profile_id,
    client_id: row.client_profile_id,
    driver_name: driver ? driver.full_name : null,
    driver_phone: driver ? driver.phone : null,
    driver_type: driverType,
    driver_type_label: driverType === 'subcontractor' ? 'Sous-traitant' : 'Interne',
    client_name: client ? client.full_name : null,
    vehicle_id: row.vehicle_id || driver?.vehicle_id || null,
    vehicle,
    vehicle_label: vehicle ? vehicle.label : '-',
    subcontractor_price: subcontractorPrice,
    subcontractor_price_label: formatMoney(subcontractorPrice),
    waiting_amount: waitingAmount,
    waiting_amount_label: formatMoney(waitingAmount),
    subcontractor_total: subcontractorTotal,
    subcontractor_total_label: formatMoney(subcontractorTotal),
    started_at: row.started_at,
    completed_at: row.ended_at,
    km_total: Number(row.total_km || 0)
  };
}

function normalizeStop(row) {
  return {
    ...row,
    stop_type: row.stop_kind,
    street_no: row.street_number,
    address: buildAddress(row),
    proofs: []
  };
}

async function fetchTours(where = {}, options = {}) {
  const limit = options.limit || null;
  const includeArchived = Boolean(options.includeArchived);

  let query = admin
    .from('tours')
    .select('*')
    .order('tour_date', { ascending: false })
    .order('created_at', { ascending: false });

  if (!includeArchived) {
    query = query.eq('is_archived', false);
  }

  Object.entries(where).forEach(([key, value]) => {
    if (Array.isArray(value)) query = query.in(key, value);
    else if (value !== undefined && value !== null && value !== '') query = query.eq(key, value);
  });

  if (limit) query = query.limit(limit);

  const { data, error } = await query;
  if (error) throw error;

  const profilesMap = await fetchProfilesMap();
  const vehiclesMap = await fetchVehiclesMap();
  return (data || []).map((row) => normalizeTour(row, profilesMap, vehiclesMap));
}

async function fetchTourById(tourId) {
  const { data, error } = await admin.from('tours').select('*').eq('id', tourId).maybeSingle();
  if (error) throw error;
  if (!data) return null;

  const profilesMap = await fetchProfilesMap();
  const vehiclesMap = await fetchVehiclesMap();
  const tour = normalizeTour(data, profilesMap, vehiclesMap);

  const { data: stopsRaw, error: stopsError } = await admin
    .from('tour_stops')
    .select('*')
    .eq('tour_id', tourId)
    .order('sequence_no', { ascending: true });
  if (stopsError) throw stopsError;

  const stops = (stopsRaw || []).map(normalizeStop);
  const stopIds = stops.map((s) => s.id);

  const proofsByStop = {};
  if (stopIds.length) {
    const { data: proofsRaw, error: proofsError } = await admin
      .from('proof_photos')
      .select('*')
      .in('stop_id', stopIds)
      .order('taken_at', { ascending: false });
    if (proofsError) throw proofsError;

    (proofsRaw || []).forEach((proof) => {
      if (!proofsByStop[proof.stop_id]) proofsByStop[proof.stop_id] = [];
      proofsByStop[proof.stop_id].push({
        ...proof,
        image_path: `/media/proofs/${proof.id}`
      });
    });
  }

  stops.forEach((stop) => {
    stop.proofs = proofsByStop[stop.id] || [];
    stop.proof_count = stop.proofs.length;
    stop.is_overdue =
      stop.status !== 'done' && diffMinutesFromNow(stop.arrived_at || tour.started_at) > STOP_OVERDUE_MINUTES;
  });

   const { data: routePointsRaw, error: routeError } = await admin
    .from('gps_tracking_points')
    .select('*')
    .eq('tour_id', tourId)
    .order('tracked_at', { ascending: true });
  if (routeError) throw routeError;

  const routePoints = (routePointsRaw || []).map((point) => ({
  ...point,
  created_at: point.tracked_at,
  minutes_since: diffMinutesFromNow(point.tracked_at),
  maps_link: `https://www.google.com/maps?q=${point.latitude},${point.longitude}`
}));

const latestLocation = routePoints.length ? routePoints[routePoints.length - 1] : null;

tour.stops = stops;
tour.routePoints = routePoints;
tour.latestLocation = latestLocation;

  tour.metrics = {
    totalStops: stops.length,
    doneStops: stops.filter((s) => s.status === 'done').length,
    pendingStops: stops.filter((s) => s.status === 'pending').length,
    arrivedStops: stops.filter((s) => s.status === 'arrived').length,
    proofCount: stops.reduce((sum, stop) => sum + (stop.proof_count || 0), 0),
    overdueStops: stops.filter((s) => s.is_overdue).length
  };

  tour.monitoring = {
    gpsStale: !tour.latestLocation || tour.latestLocation.minutes_since > DEFAULT_GPS_STALE_MINUTES,
    gpsLastSeenMinutes: tour.latestLocation ? tour.latestLocation.minutes_since : null
  };

  return tour;
}

async function createClientReport(tourId) {
  const tour = await fetchTourById(tourId);
  if (!tour) return;

  const summaryText = [
    `Début : ${formatDateTime(tour.started_at) || '-'}`,
    `Fin : ${formatDateTime(tour.completed_at) || '-'}`,
    `Km : ${tour.km_total || 0}`,
    `Arrêts terminés : ${tour.metrics.doneStops}/${tour.metrics.totalStops}`,
    `Preuves : ${tour.metrics.proofCount}`
  ].join(' | ');

  const payload = {
    tour_id: tourId,
    started_at: tour.started_at,
    ended_at: tour.completed_at,
    total_km: tour.km_total || 0,
    summary_text: summaryText,
    updated_at: nowIso()
  };

  const { error } = await admin.from('client_reports').upsert(payload, { onConflict: 'tour_id' });
  if (error) throw error;
}

async function cloneTourWithStops(sourceTour, targetDate, createdByProfileId, templateId = null) {
  const insertTourPayload = {
    template_id: templateId,
    reference_code: `${sourceTour.reference}${targetDate !== sourceTour.date ? `-${targetDate}` : ''}`,
    title: sourceTour.title || sourceTour.reference,
    tour_date: targetDate,
    client_profile_id: sourceTour.client_id || null,
    assigned_driver_profile_id: sourceTour.driver_id || null,
    driver_type: sourceTour.driver_type || 'internal',
    vehicle_id: sourceTour.vehicle_id || null,
    subcontractor_price: sourceTour.driver_type === 'subcontractor' ? Number(sourceTour.subcontractor_price || 0) : 0,
    status: 'assigned',
    notes: sourceTour.notes || null,
    created_by_profile_id: createdByProfileId || null
  };

  const { data: createdTour, error: createTourError } = await admin
    .from('tours')
    .insert(insertTourPayload)
    .select('*')
    .single();
  if (createTourError) throw createTourError;

  if (sourceTour.stops && sourceTour.stops.length) {
    const stopPayloads = sourceTour.stops.map((stop) => ({
      tour_id: createdTour.id,
      sequence_no: stop.sequence_no,
      stop_kind: stop.stop_type,
      status: 'pending',
      name: stop.name,
      street: stop.street || null,
      street_number: stop.street_no || stop.street_number || null,
      postal_code: stop.postal_code || null,
      city: stop.city || null,
      full_address: stop.address || buildAddress(stop),
      instructions: stop.instructions || null,
      contact_name: stop.contact_name || null,
      contact_phone: stop.contact_phone || null,
      latitude: stop.latitude || null,
      longitude: stop.longitude || null
    }));

    const { error: stopError } = await admin.from('tour_stops').insert(stopPayloads);
    if (stopError) throw stopError;
  }

  return createdTour;
}

async function createTemplateFromTour(sourceTourId, form, createdByProfileId) {
  const sourceTour = await fetchTourById(sourceTourId);
  if (!sourceTour) throw new Error('Tournée source introuvable');

  const recurrence = form.frequency === 'weekly' ? 'weekly' : 'daily';
  const recurrenceWeekdays = recurrence === 'weekly' ? mapWeekdaysInput(form.weekdays) : [];

  const { data: template, error: templateError } = await admin
    .from('route_templates')
    .insert({
      name: form.name,
      description: sourceTour.reference,
      client_profile_id: sourceTour.client_id || null,
      assigned_driver_profile_id: sourceTour.driver_id || null,
      recurrence,
      recurrence_weekdays: recurrenceWeekdays,
      recurrence_start_date: form.start_date,
      recurrence_end_date: form.end_date || null,
      is_active: true,
      created_by_profile_id: createdByProfileId || null
    })
    .select('*')
    .single();
  if (templateError) throw templateError;

  if (sourceTour.stops.length) {
    const payloads = sourceTour.stops.map((stop) => ({
      template_id: template.id,
      sequence_no: stop.sequence_no,
      stop_kind: stop.stop_type,
      name: stop.name,
      street: stop.street || null,
      street_number: stop.street_no || stop.street_number || null,
      postal_code: stop.postal_code || null,
      city: stop.city || null,
      full_address: stop.address,
      instructions: stop.instructions || null,
      contact_name: stop.contact_name || null,
      contact_phone: stop.contact_phone || null,
      latitude: stop.latitude || null,
      longitude: stop.longitude || null
    }));

    const { error: stopError } = await admin.from('route_template_stops').insert(payloads);
    if (stopError) throw stopError;
  }

  return template;
}

function templateMatchesDate(template, dateStr) {
  if (!template.is_active) return false;
  if (template.recurrence_start_date && dateStr < template.recurrence_start_date) return false;
  if (template.recurrence_end_date && dateStr > template.recurrence_end_date) return false;
  if (template.recurrence === 'daily') return true;

  if (template.recurrence === 'weekly') {
    const d = new Date(`${dateStr}T00:00:00Z`);
    const weekday = d.getUTCDay();
    const allowed = template.recurrence_weekdays || [];
    return allowed.length ? allowed.includes(weekday) : true;
  }

  return false;
}

async function generateToursFromTemplate(templateId, rangeStart, rangeEnd, createdByProfileId) {
  const { data: template, error: templateError } = await admin
    .from('route_templates')
    .select('*')
    .eq('id', templateId)
    .single();
  if (templateError) throw templateError;

  const { data: stopsRaw, error: stopsError } = await admin
    .from('route_template_stops')
    .select('*')
    .eq('template_id', templateId)
    .order('sequence_no', { ascending: true });
  if (stopsError) throw stopsError;

  const generatedDates = dateRange(rangeStart, rangeEnd).filter((dateStr) =>
    templateMatchesDate(template, dateStr)
  );

  let createdCount = 0;

  for (const dateStr of generatedDates) {
    const { data: existing, error: existingError } = await admin
      .from('tours')
      .select('id')
      .eq('template_id', template.id)
      .eq('tour_date', dateStr)
      .limit(1)
      .maybeSingle();
    if (existingError) throw existingError;
    if (existing) continue;

    const { data: newTour, error: newTourError } = await admin
      .from('tours')
      .insert({
        template_id: template.id,
        reference_code: `${template.name}-${dateStr}`,
        title: template.name,
        tour_date: dateStr,
        client_profile_id: template.client_profile_id || null,
        assigned_driver_profile_id: template.assigned_driver_profile_id || null,
        status: 'assigned',
        notes: `Généré depuis récurrence ${template.name}`,
        created_by_profile_id: createdByProfileId || null
      })
      .select('*')
      .single();
    if (newTourError) throw newTourError;

    if (stopsRaw && stopsRaw.length) {
      const payloads = stopsRaw.map((stop) => ({
        tour_id: newTour.id,
        sequence_no: stop.sequence_no,
        stop_kind: stop.stop_kind,
        status: 'pending',
        name: stop.name,
        street: stop.street || null,
        street_number: stop.street_number || null,
        postal_code: stop.postal_code || null,
        city: stop.city || null,
        full_address: stop.full_address || buildAddress(stop),
        instructions: stop.instructions || null,
        contact_name: stop.contact_name || null,
        contact_phone: stop.contact_phone || null,
        latitude: stop.latitude || null,
        longitude: stop.longitude || null
      }));

      const { error: insertStopsError } = await admin.from('tour_stops').insert(payloads);
      if (insertStopsError) throw insertStopsError;
    }

    createdCount += 1;
  }

  return createdCount;
}

async function buildDispatchDashboard() {
  const profiles = await fetchAllProfiles();
  const tours = await fetchTours();
  const todayStr = today();
  const toursToday = tours.filter((item) => item.date === todayStr);
  const activeTours = tours.filter((item) => item.status === 'in_progress');
  const completedToday = tours.filter((item) => item.date === todayStr && item.status === 'completed');

  const activeTourDetails = await Promise.all(activeTours.map((tour) => fetchTourById(tour.id)));

  const alerts = [];
  for (const tour of activeTourDetails) {
    if (!tour) continue;
    if (tour.monitoring.gpsStale) {
      alerts.push({
        level: 'danger',
        title: 'GPS inactif ou trop ancien',
        text: `${tour.reference} · ${tour.driver_name || 'chauffeur non assigné'} · dernière position ${
          tour.monitoring.gpsLastSeenMinutes == null ? 'jamais reçue' : `il y a ${tour.monitoring.gpsLastSeenMinutes} min`
        }`,
        href: `/dispatch/tours/${tour.id}`
      });
    }

    if (tour.metrics.overdueStops > 0) {
      alerts.push({
        level: 'warning',
        title: 'Arrêt potentiellement bloqué',
        text: `${tour.reference} · ${tour.metrics.overdueStops} arrêt(s) en retard ou trop longs`,
        href: `/dispatch/tours/${tour.id}`
      });
    }
  }

  const proofsTodayCount = activeTourDetails.reduce((sum, tour) => sum + (tour?.metrics.proofCount || 0), 0);
  const noDriverCount = toursToday.filter((tour) => !tour.driver_id).length;

  return {
    counts: {
      users: profiles.length,
      toursToday: toursToday.length,
      activeTours: activeTours.length,
      completedToday: completedToday.length,
      noDriverToday: noDriverCount,
      alerts: alerts.length,
      proofsActiveTours: proofsTodayCount
    },
    alerts: alerts.slice(0, 10),
    tours: tours.slice(0, 8),
    activeTourDetails: activeTourDetails.slice(0, 6)
  };
}

app.use(ensureSupabaseConfigured);

app.use((req, res, next) => {
  res.locals.user = req.session.user || null;
  res.locals.flash = req.session.flash || '';
  delete req.session.flash;
  res.locals.formatDateTime = formatDateTime;
  res.locals.formatMoney = formatMoney;
  res.locals.today = today;
  next();
});

function requireAuth(req, res, next) {
  if (!req.session.user) return res.redirect('/login');
  next();
}

function requireRole(...roles) {
  return (req, res, next) => {
    if (!req.session.user) return res.redirect('/login');
    if (!roles.includes(req.session.user.role)) return res.status(403).send('Accès refusé');
    next();
  };
}

app.get('/', (req, res) => {
  if (!req.session.user) return res.redirect('/login');
  if (req.session.user.role === 'dispatch') return res.redirect('/dispatch');
  if (req.session.user.role === 'driver') return res.redirect('/driver');
  if (req.session.user.role === 'client') return res.redirect('/client');
  return res.redirect('/login');
});

app.get('/login', (req, res) => {
  if (req.session.user) return res.redirect('/');
  res.render('login', { title: 'Connexion' });
});

app.post('/login', async (req, res) => {
  try {
    const username = String(req.body.username || '').trim();
    const password = String(req.body.password || '');

    if (!username || !password) {
      flash(req, 'Nom d’utilisateur et mot de passe requis.');
      return res.redirect('/login');
    }

    const profile = await getProfileByUsername(username);
    if (!profile || !profile.is_active || !profile.auth_user_id) {
      flash(req, 'Compte introuvable ou inactif.');
      return res.redirect('/login');
    }

    const { data: authInfo, error: authInfoError } = await admin.auth.admin.getUserById(
      profile.auth_user_id
    );

    if (authInfoError || !authInfo?.user?.email) {
      flash(req, 'Impossible de récupérer le compte auth Supabase.');
      return res.redirect('/login');
    }

    const authClient = createAuthClient();
    const { data: signInData, error: signInError } = await authClient.auth.signInWithPassword({
      email: authInfo.user.email,
      password
    });

    if (signInError || !signInData?.user) {
      flash(req, `Erreur login Supabase: ${signInError?.message || 'identifiants invalides'}`);
      return res.redirect('/login');
    }

    req.session.user = {
      profile_id: profile.id,
      auth_user_id: profile.auth_user_id,
      username: profile.username,
      full_name: profile.full_name,
      role: profile.role,
      email: authInfo.user.email
    };

    return res.redirect('/');
  } catch (error) {
    console.error('[LOGIN] fatal =', error);
    flash(req, `Erreur de connexion: ${error.message}`);
    return res.redirect('/login');
  }
});

app.post('/logout', (req, res) => {
  req.session = null;
  res.redirect('/login');
});

app.get('/logout', (req, res) => {
  req.session = null;
  res.redirect('/login');
});

app.get('/dispatch', requireRole('dispatch'), async (req, res) => {
  try {
    const dashboard = await buildDispatchDashboard();
    res.render('dispatch/dashboard', {
      title: 'Dashboard dispatch',
      counts: dashboard.counts,
      tours: dashboard.tours,
      alerts: dashboard.alerts,
      activeTourDetails: dashboard.activeTourDetails
    });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur dashboard dispatch');
  }
});

app.get('/dispatch/users', requireRole('dispatch'), async (req, res) => {
  try {
    const users = await fetchAllProfiles();
    const vehicles = await fetchAllVehicles();
    res.render('dispatch/users', { title: 'Utilisateurs', users, vehicles });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur utilisateurs');
  }
});

app.post('/dispatch/users', requireRole('dispatch'), async (req, res) => {
  try {
    const username = String(req.body.username || '').trim();
    const fullName = String(req.body.full_name || '').trim();
    const password = String(req.body.password || '');
    const role = String(req.body.role || 'driver');
    const phone = String(req.body.phone || '').trim() || null;
    const company = String(req.body.company || 'InterGlobe').trim() || 'InterGlobe';
    const driverType = req.body.driver_type === 'subcontractor' ? 'subcontractor' : 'internal';
    const vehicleId = req.body.vehicle_id || null;
    const isActive = Boolean(req.body.active);

    if (!username || !fullName || !password || !role) {
      flash(req, 'Tous les champs requis doivent être remplis.');
      return res.redirect('/dispatch/users');
    }

    const email = `${username.toLowerCase()}@interglobe-tracker.local`;
    const { data: createdUser, error: createUserError } = await admin.auth.admin.createUser({
      email,
      password,
      email_confirm: true,
      user_metadata: { username, full_name: fullName, role }
    });
    if (createUserError) throw createUserError;

    const profilePayload = {
      auth_user_id: createdUser.user.id,
      username,
      full_name: fullName,
      role,
      phone,
      company,
      is_active: isActive,
      must_change_password: false,
      created_by_profile_id: req.session.user.profile_id
    };

    if (role === 'driver') {
      profilePayload.driver_type = driverType;
      profilePayload.vehicle_id = vehicleId;
    }

    const { error: profileError } = await admin.from('profiles').insert(profilePayload);
    if (profileError) throw profileError;

    flash(req, 'Utilisateur créé.');
    res.redirect('/dispatch/users');
  } catch (error) {
    console.error(error);
    flash(req, `Erreur création utilisateur : ${error.message}`);
    res.redirect('/dispatch/users');
  }
});

app.post('/dispatch/users/:id/toggle', requireRole('dispatch'), async (req, res) => {
  try {
    const { data: existing, error: existingError } = await admin
      .from('profiles')
      .select('id, is_active')
      .eq('id', req.params.id)
      .single();
    if (existingError) throw existingError;

    const { error } = await admin
      .from('profiles')
      .update({ is_active: !existing.is_active })
      .eq('id', req.params.id);
    if (error) throw error;

    flash(req, 'Statut du compte mis à jour.');
    res.redirect('/dispatch/users');
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur modification utilisateur.');
    res.redirect('/dispatch/users');
  }
});

app.post('/dispatch/users/:id/delete', requireRole('dispatch'), async (req, res) => {
  try {
    const profileId = req.params.id;

    const { data: profile, error: profileFetchError } = await admin
      .from('profiles')
      .select('*')
      .eq('id', profileId)
      .single();
    if (profileFetchError) throw profileFetchError;

    if (profile.username === 'dispatch') {
      flash(req, 'Le compte dispatch principal ne peut pas être supprimé.');
      return res.redirect('/dispatch/users');
    }

    const { error: toursDriverError } = await admin
      .from('tours')
      .update({ assigned_driver_profile_id: null })
      .eq('assigned_driver_profile_id', profileId);
    if (toursDriverError) throw toursDriverError;

    const { error: toursClientError } = await admin
      .from('tours')
      .update({ client_profile_id: null })
      .eq('client_profile_id', profileId);
    if (toursClientError) throw toursClientError;

    const { error: proofError } = await admin
      .from('proof_photos')
      .delete()
      .eq('driver_profile_id', profileId);
    if (proofError) throw proofError;

    const { error: gpsError } = await admin
      .from('gps_tracking_points')
      .delete()
      .eq('driver_profile_id', profileId);
    if (gpsError) throw gpsError;

    const { error: profileDeleteError } = await admin
      .from('profiles')
      .delete()
      .eq('id', profileId);
    if (profileDeleteError) throw profileDeleteError;

    if (profile.auth_user_id) {
      const { error: authDeleteError } = await admin.auth.admin.deleteUser(profile.auth_user_id);
      if (authDeleteError) throw authDeleteError;
    }

    flash(req, 'Utilisateur supprimé.');
    res.redirect('/dispatch/users');
  } catch (error) {
    console.error(error);
    flash(req, `Erreur suppression utilisateur : ${error.message}`);
    res.redirect('/dispatch/users');
  }
});


app.get('/dispatch/vehicles', requireRole('dispatch'), async (req, res) => {
  try {
    const vehicles = await fetchAllVehicles();
    const profiles = await fetchAllProfiles();
    const drivers = profiles.filter((item) => item.role === 'driver');

    const vehiclesWithDrivers = vehicles.map((vehicle) => {
      const assignedDrivers = drivers.filter((driver) => String(driver.vehicle_id || '') === String(vehicle.id));
      return {
        ...vehicle,
        assigned_drivers: assignedDrivers,
        assigned_driver_names: assignedDrivers.map((driver) => driver.full_name).join(', ')
      };
    });

    res.render('dispatch/vehicles', {
      title: 'Véhicules',
      vehicles: vehiclesWithDrivers,
      drivers
    });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur véhicules');
  }
});

app.post('/dispatch/vehicles', requireRole('dispatch'), async (req, res) => {
  try {
    const plate = String(req.body.plate || '').trim().toUpperCase();
    const brand = String(req.body.brand || '').trim() || null;
    const model = String(req.body.model || '').trim() || null;

    if (!plate) {
      flash(req, 'La plaque est obligatoire.');
      return res.redirect('/dispatch/vehicles');
    }

    const payload = {
      plate,
      brand,
      model,
      length_m: parseNumberOrNull(req.body.length_m),
      width_m: parseNumberOrNull(req.body.width_m),
      height_m: parseNumberOrNull(req.body.height_m),
      mma_kg: parseNumberOrNull(req.body.mma_kg),
      payload_kg: parseNumberOrNull(req.body.payload_kg),
      status: req.body.status || 'available',
      notes: String(req.body.notes || '').trim() || null
    };

    const { error } = await admin.from('vehicles').insert(payload);
    if (error) throw error;

    flash(req, 'Véhicule créé.');
    res.redirect('/dispatch/vehicles');
  } catch (error) {
    console.error(error);
    flash(req, `Erreur création véhicule : ${error.message}`);
    res.redirect('/dispatch/vehicles');
  }
});

app.post('/dispatch/vehicles/:id/update', requireRole('dispatch'), async (req, res) => {
  try {
    const plate = String(req.body.plate || '').trim().toUpperCase();

    if (!plate) {
      flash(req, 'La plaque est obligatoire.');
      return res.redirect('/dispatch/vehicles');
    }

    const payload = {
      plate,
      brand: String(req.body.brand || '').trim() || null,
      model: String(req.body.model || '').trim() || null,
      length_m: parseNumberOrNull(req.body.length_m),
      width_m: parseNumberOrNull(req.body.width_m),
      height_m: parseNumberOrNull(req.body.height_m),
      mma_kg: parseNumberOrNull(req.body.mma_kg),
      payload_kg: parseNumberOrNull(req.body.payload_kg),
      status: req.body.status || 'available',
      notes: String(req.body.notes || '').trim() || null
    };

    const { error } = await admin.from('vehicles').update(payload).eq('id', req.params.id);
    if (error) throw error;

    flash(req, 'Véhicule mis à jour.');
    res.redirect('/dispatch/vehicles');
  } catch (error) {
    console.error(error);
    flash(req, `Erreur modification véhicule : ${error.message}`);
    res.redirect('/dispatch/vehicles');
  }
});

app.post('/dispatch/vehicles/:id/delete', requireRole('dispatch'), async (req, res) => {
  try {
    const vehicleId = req.params.id;

    const { error: profilesError } = await admin
      .from('profiles')
      .update({ vehicle_id: null })
      .eq('vehicle_id', vehicleId);
    if (profilesError) throw profilesError;

    const { error: toursError } = await admin
      .from('tours')
      .update({ vehicle_id: null })
      .eq('vehicle_id', vehicleId);
    if (toursError && !String(toursError.message || '').includes('vehicle_id')) throw toursError;

    const { error } = await admin.from('vehicles').delete().eq('id', vehicleId);
    if (error) throw error;

    flash(req, 'Véhicule supprimé.');
    res.redirect('/dispatch/vehicles');
  } catch (error) {
    console.error(error);
    flash(req, `Erreur suppression véhicule : ${error.message}`);
    res.redirect('/dispatch/vehicles');
  }
});

app.get('/dispatch/subcontractors/recap', requireRole('dispatch'), async (req, res) => {
  try {
    const recap = await fetchSubcontractorMonthlyRecaps(req.query.month);

    res.render('dispatch/subcontractor_recaps', {
      title: 'Récapitulatif sous-traitants',
      recap,
      selectedMonth: recap.month
    });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur récapitulatif sous-traitants');
  }
});

app.get('/dispatch/subcontractors/recap/export.csv', requireRole('dispatch'), async (req, res) => {
  try {
    const recap = await fetchSubcontractorMonthlyRecaps(req.query.month);
    const csv = stringify(
      recap.tours.map((tour) => ({
        mois: recap.month,
        chauffeur: tour.driver_name || '',
        date: tour.date,
        reference: tour.reference,
        client: tour.client_name || '',
        vehicle: tour.vehicle_label || '',
        status: tour.status,
        prix_course: Number(tour.subcontractor_price || 0),
        montant_attente: Number(tour.waiting_amount || 0),
        total_a_facturer: Number(tour.subcontractor_total || 0),
        notes: tour.notes || ''
      })),
      { header: true }
    );

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="recap-sous-traitants-${recap.month}.csv"`);
    res.send(csv);
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur export récapitulatif sous-traitants');
  }
});


app.get('/dispatch/subcontractors/recap/export.pdf', requireRole('dispatch'), async (req, res) => {
  try {
    const recap = await fetchSubcontractorMonthlyRecaps(req.query.month);
    buildDispatchSubcontractorRecapPdf(res, recap);
  } catch (error) {
    console.error(error);
    if (error.code === 'PDFKIT_MISSING') {
      return res.status(500).send(error.message);
    }
    return res.status(500).send('Erreur export PDF récapitulatif sous-traitants');
  }
});

app.post('/dispatch/tours/:id/waiting-amount', requireRole('dispatch'), async (req, res) => {
  try {
    const waitingAmount = Number(parseNumberOrNull(req.body.waiting_amount) || 0);
    if (waitingAmount < 0) {
      flash(req, 'Le montant d’attente ne peut pas être négatif.');
      return res.redirect(req.get('referer') || `/dispatch/tours/${req.params.id}`);
    }

    const { error } = await admin
      .from('tours')
      .update({ waiting_amount: waitingAmount })
      .eq('id', req.params.id);
    if (error) throw error;

    flash(req, 'Montant d’attente mis à jour.');
    res.redirect(req.get('referer') || `/dispatch/tours/${req.params.id}`);
  } catch (error) {
    console.error(error);
    flash(req, `Erreur montant d’attente : ${error.message}`);
    res.redirect(req.get('referer') || '/dispatch/tours');
  }
});

app.get('/dispatch/tours', requireRole('dispatch'), async (req, res) => {
  try {
    const filters = {
      tour_date: req.query.date || undefined,
      assigned_driver_profile_id: req.query.driver_id || undefined,
      client_profile_id: req.query.client_id || undefined,
      status: req.query.status || undefined
    };

    const tours = await fetchTours(filters);
    const profiles = await fetchAllProfiles();
    const drivers = profiles.filter((item) => item.role === 'driver' && item.is_active);
    const clients = profiles.filter((item) => item.role === 'client' && item.is_active);

    res.render('dispatch/tours', {
      title: 'Tournées',
      tours,
      drivers,
      clients,
      today,
      filters: {
        date: req.query.date || '',
        driver_id: req.query.driver_id || '',
        client_id: req.query.client_id || '',
        status: req.query.status || ''
      }
    });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur tournées');
  }
});

app.get('/dispatch/tours/archives', requireRole('dispatch'), async (req, res) => {
  try {
    const profiles = await fetchAllProfiles();
    const drivers = profiles.filter((item) => item.role === 'driver' && item.is_active);
    const clients = profiles.filter((item) => item.role === 'client' && item.is_active);

    const filters = {
      tour_date: req.query.date || undefined,
      assigned_driver_profile_id: req.query.driver_id || undefined,
      client_profile_id: req.query.client_id || undefined,
      status: req.query.status || undefined,
      is_archived: true
    };

    const tours = await fetchTours(filters, { includeArchived: true });

    res.render('dispatch/tours_archives', {
      title: 'Archives',
      tours,
      drivers,
      clients,
      today,
      filters: {
        date: req.query.date || '',
        driver_id: req.query.driver_id || '',
        client_id: req.query.client_id || '',
        status: req.query.status || ''
      }
    });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur archives tournées');
  }
});

app.get('/dispatch/tours/export.csv', requireRole('dispatch'), async (req, res) => {
  try {
    const filters = {
      tour_date: req.query.date || undefined,
      assigned_driver_profile_id: req.query.driver_id || undefined,
      client_profile_id: req.query.client_id || undefined,
      status: req.query.status || undefined
    };
    const tours = await fetchTours(filters);

    const csv = stringify(
      tours.map((tour) => ({
        reference: tour.reference,
        date: tour.date,
        driver: tour.driver_name || '',
        driver_type: tour.driver_type_label || '',
        vehicle: tour.vehicle_label || '',
        client: tour.client_name || '',
        subcontractor_price: tour.driver_type === 'subcontractor' ? Number(tour.subcontractor_price || 0) : '',
        waiting_amount: tour.driver_type === 'subcontractor' ? Number(tour.waiting_amount || 0) : '',
        subcontractor_total: tour.driver_type === 'subcontractor' ? Number(tour.subcontractor_total || 0) : '',
        status: tour.status,
        started_at: tour.started_at || '',
        completed_at: tour.completed_at || '',
        total_km: tour.km_total || 0,
        notes: tour.notes || ''
      })),
      { header: true }
    );

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="tours-export.csv"');
    res.send(csv);
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur export tournées');
  }
});

app.post('/dispatch/tours', requireRole('dispatch'), async (req, res) => {
  try {
    const driverId = req.body.driver_id || null;
    const assignment = await getDriverAssignmentInfo(driverId);
    const subcontractorPrice = assignment.driver_type === 'subcontractor'
      ? Number(parseNumberOrNull(req.body.subcontractor_price) || 0)
      : 0;

    const payload = {
      reference_code: String(req.body.reference || '').trim(),
      title: String(req.body.reference || '').trim() || 'Tournée',
      tour_date: req.body.date,
      assigned_driver_profile_id: driverId,
      client_profile_id: req.body.client_id || null,
      driver_type: assignment.driver_type,
      vehicle_id: assignment.vehicle_id,
      subcontractor_price: subcontractorPrice,
      waiting_amount: 0,
      status: 'assigned',
      notes: String(req.body.notes || '').trim() || null,
      created_by_profile_id: req.session.user.profile_id
    };

    if (assignment.driver_type === 'subcontractor' && subcontractorPrice <= 0) {
      flash(req, 'Indique le prix convenu pour le sous-traitant.');
      return res.redirect('/dispatch/tours');
    }

    const { error } = await admin.from('tours').insert(payload);
    if (error) throw error;

    flash(req, 'Tournée créée.');
    res.redirect('/dispatch/tours');
  } catch (error) {
    console.error(error);
    flash(req, `Erreur création tournée : ${error.message}`);
    res.redirect('/dispatch/tours');
  }
});

app.post('/dispatch/tours/duplicate-day', requireRole('dispatch'), async (req, res) => {
  try {
    const sourceDate = req.body.source_date;
    const targetDate = req.body.target_date;
    const sourceTours = await fetchTours({ tour_date: sourceDate });

    if (!sourceTours.length) {
      flash(req, 'Aucune tournée trouvée pour le jour source.');
      return res.redirect('/dispatch/tours');
    }

    for (const tour of sourceTours) {
      const fullTour = await fetchTourById(tour.id);
      await cloneTourWithStops(fullTour, targetDate, req.session.user.profile_id);
    }

    flash(req, 'Planning dupliqué.');
    return res.redirect('/dispatch/tours');
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur duplication planning.');
    return res.redirect('/dispatch/tours');
  }
});

app.post('/dispatch/tours/:id/delete', requireRole('dispatch'), async (req, res) => {
  try {
    const tourId = req.params.id;

    const { error: proofsError } = await admin
      .from('proof_photos')
      .delete()
      .eq('tour_id', tourId);
    if (proofsError) throw proofsError;

    const { error: gpsError } = await admin
      .from('gps_tracking_points')
      .delete()
      .eq('tour_id', tourId);
    if (gpsError) throw gpsError;

    const { error: reportsError } = await admin
      .from('client_reports')
      .delete()
      .eq('tour_id', tourId);
    if (reportsError) throw reportsError;

    const { error: stopsError } = await admin
      .from('tour_stops')
      .delete()
      .eq('tour_id', tourId);
    if (stopsError) throw stopsError;

    const { error: tourError } = await admin
      .from('tours')
      .delete()
      .eq('id', tourId);
    if (tourError) throw tourError;

    flash(req, 'Tournée supprimée.');
    res.redirect('/dispatch/tours');
  } catch (error) {
    console.error(error);
    flash(req, `Erreur suppression tournée : ${error.message}`);
    res.redirect('/dispatch/tours');
  }
});

app.post('/dispatch/tours/:id/archive', requireRole('dispatch'), async (req, res) => {
  try {
    const { data: tour, error: fetchError } = await admin
      .from('tours')
      .select('id, status')
      .eq('id', req.params.id)
      .single();
    if (fetchError) throw fetchError;

    if (tour.status !== 'completed') {
      flash(req, 'Seules les tournées terminées peuvent être archivées.');
      return res.redirect('/dispatch/tours');
    }

    const { error } = await admin
      .from('tours')
      .update({ is_archived: true })
      .eq('id', req.params.id);
    if (error) throw error;

    flash(req, 'Tournée archivée.');
    res.redirect('/dispatch/tours');
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur archivage tournée.');
    res.redirect('/dispatch/tours');
  }
});

app.post('/dispatch/tours/:id/unarchive', requireRole('dispatch'), async (req, res) => {
  try {
    const { error } = await admin
      .from('tours')
      .update({ is_archived: false })
      .eq('id', req.params.id);
    if (error) throw error;

    flash(req, 'Tournée désarchivée.');
    res.redirect('/dispatch/tours/archives');
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur désarchivage tournée.');
    res.redirect('/dispatch/tours/archives');
  }
});

app.get('/dispatch/tours/:id', requireRole('dispatch'), async (req, res) => {
  try {
    const tour = await fetchTourById(req.params.id);
    if (!tour) return res.status(404).send('Tournée introuvable');
    res.render('dispatch/tour_detail', { title: tour.reference, tour });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur détail tournée');
  }
});

app.get('/dispatch/tours/:id/export.csv', requireRole('dispatch'), async (req, res) => {
  try {
    const tour = await fetchTourById(req.params.id);
    if (!tour) return res.status(404).send('Tournée introuvable');

    const csv = stringify(
      tour.stops.map((stop) => ({
        sequence_no: stop.sequence_no,
        stop_type: stop.stop_type,
        status: stop.status,
        name: stop.name,
        address: stop.address,
        contact_name: stop.contact_name || '',
        contact_phone: stop.contact_phone || '',
        arrived_at: stop.arrived_at || '',
        departed_at: stop.departed_at || '',
        proof_count: stop.proof_count || 0,
        instructions: stop.instructions || ''
      })),
      { header: true }
    );

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${tour.reference}-stops.csv"`);
    res.send(csv);
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur export détails tournée');
  }
});

app.get('/dispatch/tours/:id/edit', requireRole('dispatch'), async (req, res) => {
  try {
    const tour = await fetchTourById(req.params.id);
    if (!tour) return res.status(404).send('Tournée introuvable');

    const profiles = await fetchAllProfiles();
    const drivers = profiles.filter((item) => item.role === 'driver');
    const clients = profiles.filter((item) => item.role === 'client');

    res.render('dispatch/tour_form', {
      title: 'Modifier tournée',
      tour,
      drivers,
      clients
    });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur édition tournée');
  }
});

app.post('/dispatch/tours/:id/edit', requireRole('dispatch'), async (req, res) => {
  try {
    const driverId = req.body.driver_id || null;
    const assignment = await getDriverAssignmentInfo(driverId);
    const subcontractorPrice = assignment.driver_type === 'subcontractor'
      ? Number(parseNumberOrNull(req.body.subcontractor_price) || 0)
      : 0;

    if (assignment.driver_type === 'subcontractor' && subcontractorPrice <= 0) {
      flash(req, 'Indique le prix convenu pour le sous-traitant.');
      return res.redirect(`/dispatch/tours/${req.params.id}/edit`);
    }

    const { error } = await admin
      .from('tours')
      .update({
        reference_code: String(req.body.reference || '').trim(),
        title: String(req.body.reference || '').trim() || 'Tournée',
        tour_date: req.body.date,
        assigned_driver_profile_id: driverId,
        client_profile_id: req.body.client_id || null,
        driver_type: assignment.driver_type,
        vehicle_id: assignment.vehicle_id,
        subcontractor_price: subcontractorPrice,
        status: req.body.status || 'draft',
        notes: String(req.body.notes || '').trim() || null,
        ended_at: req.body.status === 'completed' ? nowIso() : null
      })
      .eq('id', req.params.id);
    if (error) throw error;

    flash(req, 'Tournée mise à jour.');
    res.redirect(`/dispatch/tours/${req.params.id}`);
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur modification tournée.');
    res.redirect(`/dispatch/tours/${req.params.id}/edit`);
  }
});

app.post('/dispatch/tours/:id/stops', requireRole('dispatch'), async (req, res) => {
  try {
    const payload = {
      tour_id: req.params.id,
      sequence_no: Number(req.body.sequence_no || 1),
      stop_kind: req.body.stop_type,
      status: 'pending',
      name: String(req.body.name || '').trim(),
      street: String(req.body.street || '').trim() || null,
      street_number: String(req.body.street_no || '').trim() || null,
      postal_code: String(req.body.postal_code || '').trim() || null,
      city: String(req.body.city || '').trim() || null,
      full_address: [
        [String(req.body.street || '').trim(), String(req.body.street_no || '').trim()]
          .filter(Boolean)
          .join(' '),
        [String(req.body.postal_code || '').trim(), String(req.body.city || '').trim()]
          .filter(Boolean)
          .join(' ')
      ]
        .filter(Boolean)
        .join(', '),
      instructions: String(req.body.instructions || '').trim() || null,
      contact_name: String(req.body.contact_name || '').trim() || null,
      contact_phone: String(req.body.contact_phone || '').trim() || null,
      latitude: req.body.latitude || null,
      longitude: req.body.longitude || null
    };

    const { error } = await admin.from('tour_stops').insert(payload);
    if (error) throw error;

    flash(req, 'Arrêt ajouté.');
    res.redirect(`/dispatch/tours/${req.params.id}`);
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur ajout arrêt.');
    res.redirect(`/dispatch/tours/${req.params.id}`);
  }
});

app.get('/dispatch/stops/:id/edit', requireRole('dispatch'), async (req, res) => {
  try {
    const { data: stop, error } = await admin
      .from('tour_stops')
      .select('*')
      .eq('id', req.params.id)
      .single();
    if (error) throw error;

    res.render('dispatch/stop_form', {
      title: 'Modifier arrêt',
      stop: normalizeStop(stop)
    });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur édition arrêt');
  }
});

app.post('/dispatch/stops/:id/edit', requireRole('dispatch'), async (req, res) => {
  try {
    const update = {
      sequence_no: Number(req.body.sequence_no || 1),
      stop_kind: req.body.stop_type,
      name: String(req.body.name || '').trim(),
      street: String(req.body.street || '').trim() || null,
      street_number: String(req.body.street_no || '').trim() || null,
      postal_code: String(req.body.postal_code || '').trim() || null,
      city: String(req.body.city || '').trim() || null,
      full_address: [
        [String(req.body.street || '').trim(), String(req.body.street_no || '').trim()]
          .filter(Boolean)
          .join(' '),
        [String(req.body.postal_code || '').trim(), String(req.body.city || '').trim()]
          .filter(Boolean)
          .join(' ')
      ]
        .filter(Boolean)
        .join(', '),
      instructions: String(req.body.instructions || '').trim() || null,
      contact_name: String(req.body.contact_name || '').trim() || null,
      contact_phone: String(req.body.contact_phone || '').trim() || null,
      latitude: req.body.latitude || null,
      longitude: req.body.longitude || null
    };

    const { data: stop, error: fetchError } = await admin
      .from('tour_stops')
      .select('tour_id')
      .eq('id', req.params.id)
      .single();
    if (fetchError) throw fetchError;

    const { error } = await admin.from('tour_stops').update(update).eq('id', req.params.id);
    if (error) throw error;

    flash(req, 'Arrêt mis à jour.');
    res.redirect(`/dispatch/tours/${stop.tour_id}`);
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur modification arrêt.');
    res.redirect('/dispatch/tours');
  }
});

app.post('/dispatch/stops/:id/delete', requireRole('dispatch'), async (req, res) => {
  try {
    const { data: stop, error: fetchError } = await admin
      .from('tour_stops')
      .select('tour_id')
      .eq('id', req.params.id)
      .single();
    if (fetchError) throw fetchError;

    const { error } = await admin.from('tour_stops').delete().eq('id', req.params.id);
    if (error) throw error;

    flash(req, 'Arrêt supprimé.');
    res.redirect(`/dispatch/tours/${stop.tour_id}`);
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur suppression arrêt.');
    res.redirect('/dispatch/tours');
  }
});

app.get('/dispatch/recurrences', requireRole('dispatch'), async (req, res) => {
  try {
    const tours = await fetchTours();
    const { data: templatesRaw, error } = await admin
      .from('route_templates')
      .select('*')
      .order('created_at', { ascending: false });
    if (error) throw error;

    const recurrences = (templatesRaw || []).map((item) => ({
      id: item.id,
      name: item.name,
      reference: item.description || '-',
      frequency: item.recurrence,
      weekdays: weekdaysToInput(item.recurrence_weekdays),
      start_date: item.recurrence_start_date,
      end_date: item.recurrence_end_date,
      active: item.is_active
    }));

    res.render('dispatch/recurrences', {
      title: 'Récurrences',
      tours,
      recurrences,
      today
    });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur récurrences');
  }
});

app.post('/dispatch/recurrences', requireRole('dispatch'), async (req, res) => {
  try {
    await createTemplateFromTour(req.body.source_tour_id, req.body, req.session.user.profile_id);
    flash(req, 'Récurrence créée.');
    res.redirect('/dispatch/recurrences');
  } catch (error) {
    console.error(error);
    flash(req, `Erreur création récurrence : ${error.message}`);
    res.redirect('/dispatch/recurrences');
  }
});

app.post('/dispatch/recurrences/:id/update', requireRole('dispatch'), async (req, res) => {
  try {
    const recurrence = req.body.frequency === 'weekly' ? 'weekly' : 'daily';

    const { error } = await admin
      .from('route_templates')
      .update({
        name: req.body.name,
        recurrence,
        recurrence_weekdays: recurrence === 'weekly' ? mapWeekdaysInput(req.body.weekdays) : [],
        recurrence_start_date: req.body.start_date,
        recurrence_end_date: req.body.end_date || null,
        is_active: Boolean(req.body.active)
      })
      .eq('id', req.params.id);
    if (error) throw error;

    flash(req, 'Récurrence mise à jour.');
    res.redirect('/dispatch/recurrences');
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur mise à jour récurrence.');
    res.redirect('/dispatch/recurrences');
  }
});

app.post('/dispatch/recurrences/:id/generate', requireRole('dispatch'), async (req, res) => {
  try {
    const count = await generateToursFromTemplate(
      req.params.id,
      req.body.range_start,
      req.body.range_end,
      req.session.user.profile_id
    );

    flash(req, `${count} tournée(s) générée(s).`);
    res.redirect('/dispatch/recurrences');
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur génération récurrence.');
    res.redirect('/dispatch/recurrences');
  }
});


async function deleteTourCascade(tourId) {
  const { error: proofsError } = await admin
    .from('proof_photos')
    .delete()
    .eq('tour_id', tourId);
  if (proofsError) throw proofsError;

  const { error: gpsError } = await admin
    .from('gps_tracking_points')
    .delete()
    .eq('tour_id', tourId);
  if (gpsError) throw gpsError;

  const { error: reportsError } = await admin
    .from('client_reports')
    .delete()
    .eq('tour_id', tourId);
  if (reportsError) throw reportsError;

  const { error: stopsError } = await admin
    .from('tour_stops')
    .delete()
    .eq('tour_id', tourId);
  if (stopsError) throw stopsError;

  const { error: tourError } = await admin
    .from('tours')
    .delete()
    .eq('id', tourId);
  if (tourError) throw tourError;
}

app.post('/dispatch/recurrences/:id/delete', requireRole('dispatch'), async (req, res) => {
  try {
    const templateId = req.params.id;
    const deleteScope = String(req.body.delete_scope || 'recurrence_only');

    const { data: template, error: templateError } = await admin
      .from('route_templates')
      .select('id, name')
      .eq('id', templateId)
      .maybeSingle();
    if (templateError) throw templateError;
    if (!template) {
      flash(req, 'Récurrence introuvable.');
      return res.redirect('/dispatch/recurrences');
    }

    let deletedToursCount = 0;

    if (deleteScope === 'recurrence_and_future') {
      const { data: futureTours, error: futureToursError } = await admin
        .from('tours')
        .select('id')
        .eq('template_id', templateId)
        .gte('tour_date', today())
        .in('status', ['draft', 'assigned']);
      if (futureToursError) throw futureToursError;

      for (const tour of futureTours || []) {
        await deleteTourCascade(tour.id);
        deletedToursCount += 1;
      }
    }

    const { error: detachToursError } = await admin
      .from('tours')
      .update({ template_id: null })
      .eq('template_id', templateId);
    if (detachToursError) throw detachToursError;

    const { error: stopsError } = await admin
      .from('route_template_stops')
      .delete()
      .eq('template_id', templateId);
    if (stopsError) throw stopsError;

    const { error: deleteTemplateError } = await admin
      .from('route_templates')
      .delete()
      .eq('id', templateId);
    if (deleteTemplateError) throw deleteTemplateError;

    flash(
      req,
      deleteScope === 'recurrence_and_future'
        ? `Récurrence supprimée. ${deletedToursCount} tournée(s) future(s) supprimée(s).`
        : 'Récurrence supprimée. Les tournées déjà créées sont conservées.'
    );
    return res.redirect('/dispatch/recurrences');
  } catch (error) {
    console.error(error);
    flash(req, `Erreur suppression récurrence : ${error.message}`);
    return res.redirect('/dispatch/recurrences');
  }
});

app.get('/driver', requireRole('driver'), async (req, res) => {
  try {
    const tours = await fetchTours({ assigned_driver_profile_id: req.session.user.profile_id });
    const profile = await fetchProfileById(req.session.user.profile_id);
    const currentMonth = currentMonthInput();

    res.render('driver/dashboard', {
      title: 'Mes tournées',
      tours,
      profile,
      currentMonth
    });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur tournée chauffeur');
  }
});

app.get('/driver/recap', requireRole('driver'), async (req, res) => {
  try {
    const profile = await fetchProfileById(req.session.user.profile_id);
    if (!profile || profile.driver_type !== 'subcontractor') {
      return res.status(403).send('Récapitulatif disponible uniquement pour les sous-traitants.');
    }

    const recap = await fetchDriverMonthlyRecap(req.session.user.profile_id, req.query.month);

    res.render('driver/monthly_recap', {
      title: 'Récapitulatif mensuel',
      profile,
      recap,
      selectedMonth: recap.month
    });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur récapitulatif mensuel');
  }
});

app.get('/driver/recap/export.csv', requireRole('driver'), async (req, res) => {
  try {
    const profile = await fetchProfileById(req.session.user.profile_id);
    if (!profile || profile.driver_type !== 'subcontractor') {
      return res.status(403).send('Export disponible uniquement pour les sous-traitants.');
    }

    const recap = await fetchDriverMonthlyRecap(req.session.user.profile_id, req.query.month);
    const csv = stringify(
      recap.tours.map((tour) => ({
        date: tour.date,
        reference: tour.reference,
        client: tour.client_name || '',
        vehicle: tour.vehicle_label || '',
        status: tour.status,
        km: tour.km_total || 0,
        prix_convenu: Number(tour.subcontractor_price || 0),
        montant_attente: Number(tour.waiting_amount || 0),
        total_a_facturer: Number(tour.subcontractor_total || 0),
        notes: tour.notes || ''
      })),
      { header: true }
    );

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="recap-${recap.month}.csv"`);
    res.send(csv);
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur export récapitulatif');
  }
});

app.get('/driver/tours/:id', requireRole('driver'), async (req, res) => {
  try {
    const tour = await fetchTourById(req.params.id);
    if (!tour || tour.driver_id !== req.session.user.profile_id) {
      return res.status(404).send('Tournée introuvable');
    }

    const currentStop =
      tour.stops.find((stop) => stop.status === 'arrived') ||
      tour.stops.find((stop) => stop.status === 'pending') ||
      null;

    const completedStops = tour.stops.filter((stop) => stop.status === 'done').length;
    const totalStops = tour.stops.length;

    res.render('driver/tour', {
      title: tour.reference,
      tour,
      currentStop,
      completedStops,
      totalStops
    });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur détail tournée chauffeur');
  }
});

app.post('/driver/tours/:id/start', requireRole('driver'), async (req, res) => {
  try {
    const { error } = await admin
      .from('tours')
      .update({
        status: 'in_progress',
        started_at: nowIso(),
        ended_at: null
      })
      .eq('id', req.params.id)
      .eq('assigned_driver_profile_id', req.session.user.profile_id);
    if (error) throw error;

    flash(req, 'Tournée démarrée.');
    res.redirect(`/driver/tours/${req.params.id}`);
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur démarrage tournée.');
    res.redirect(`/driver/tours/${req.params.id}`);
  }
});

app.post('/driver/tours/:id/complete', requireRole('driver'), async (req, res) => {
  try {
    const endedAt = nowIso();

    const { error } = await admin
      .from('tours')
      .update({
        status: 'completed',
        ended_at: endedAt
      })
      .eq('id', req.params.id)
      .eq('assigned_driver_profile_id', req.session.user.profile_id);
    if (error) throw error;

    await createClientReport(req.params.id);

    flash(req, 'Tournée terminée.');
    res.redirect(`/driver/tours/${req.params.id}`);
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur fin tournée.');
    res.redirect(`/driver/tours/${req.params.id}`);
  }
});

app.post('/driver/stops/:id/arrive', requireRole('driver'), async (req, res) => {
  try {
    const { data: stop, error: fetchError } = await admin
      .from('tour_stops')
      .select('id, tour_id')
      .eq('id', req.params.id)
      .single();
    if (fetchError) throw fetchError;

    const { error } = await admin
      .from('tour_stops')
      .update({ status: 'arrived', arrived_at: nowIso() })
      .eq('id', req.params.id);
    if (error) throw error;

    flash(req, 'Arrivée enregistrée.');
    res.redirect(`/driver/tours/${stop.tour_id}`);
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur arrivée arrêt.');
    res.redirect('/driver');
  }
});

app.post('/driver/stops/:id/depart', requireRole('driver'), async (req, res) => {
  try {
    const { data: stop, error: fetchError } = await admin
      .from('tour_stops')
      .select('id, tour_id')
      .eq('id', req.params.id)
      .single();
    if (fetchError) throw fetchError;

    const { data: proofs, error: proofsError } = await admin
      .from('proof_photos')
      .select('id')
      .eq('stop_id', req.params.id)
      .limit(1);
    if (proofsError) throw proofsError;

    if (!proofs || proofs.length === 0) {
      flash(req, 'Ajoute au moins une preuve de livraison avant de partir.');
      return res.redirect(`/driver/tours/${stop.tour_id}`);
    }

    const { error } = await admin
      .from('tour_stops')
      .update({ status: 'done', departed_at: nowIso() })
      .eq('id', req.params.id);
    if (error) throw error;

    flash(req, 'Départ enregistré.');
    res.redirect(`/driver/tours/${stop.tour_id}`);
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur départ arrêt.');
    res.redirect('/driver');
  }
});

app.post('/driver/stops/:id/proofs', requireRole('driver'), upload.array('photos', 10), async (req, res) => {
  try {
    const { data: stop, error: stopError } = await admin
      .from('tour_stops')
      .select('id, tour_id')
      .eq('id', req.params.id)
      .single();
    if (stopError) throw stopError;

    const files = req.files || [];
    for (const file of files) {
      const filePath = `${stop.tour_id}/${stop.id}/${Date.now()}-${sanitizeFileName(file.originalname)}`;

      const { error: uploadError } = await admin.storage.from(PROOF_BUCKET).upload(filePath, file.buffer, {
        contentType: file.mimetype,
        upsert: false
      });
      if (uploadError) throw uploadError;

      const { error: proofError } = await admin.from('proof_photos').insert({
        tour_id: stop.tour_id,
        stop_id: stop.id,
        driver_profile_id: req.session.user.profile_id,
        file_path: filePath,
        note: String(req.body.note || '').trim() || null
      });
      if (proofError) throw proofError;
    }

    flash(req, 'Preuve(s) envoyée(s).');
    res.redirect(`/driver/tours/${stop.tour_id}`);
  } catch (error) {
    console.error(error);
    flash(req, `Erreur upload preuve : ${error.message}`);
    res.redirect('/driver');
  }
});

app.post('/driver/tours/:id/location', requireRole('driver'), async (req, res) => {
  try {
    const latitude = Number(req.body.latitude);
    const longitude = Number(req.body.longitude);
    const accuracy = req.body.accuracy ? Number(req.body.accuracy) : null;

    const { data: previousPoint, error: previousError } = await admin
      .from('gps_tracking_points')
      .select('*')
      .eq('tour_id', req.params.id)
      .order('tracked_at', { ascending: false })
      .limit(1)
      .maybeSingle();
    if (previousError) throw previousError;

    const { error: insertError } = await admin.from('gps_tracking_points').insert({
      tour_id: req.params.id,
      driver_profile_id: req.session.user.profile_id,
      latitude,
      longitude,
      accuracy_meters: accuracy,
      tracked_at: nowIso()
    });
    if (insertError) throw insertError;

    let kmToAdd = 0;
    if (previousPoint) {
      kmToAdd = haversineKm(
        Number(previousPoint.latitude),
        Number(previousPoint.longitude),
        latitude,
        longitude
      );
    }

    const { data: tour, error: tourError } = await admin
      .from('tours')
      .select('total_km')
      .eq('id', req.params.id)
      .single();
    if (tourError) throw tourError;

    const newKm = Number(tour.total_km || 0) + kmToAdd;

    const { error: updateError } = await admin
      .from('tours')
      .update({ total_km: Number(newKm.toFixed(2)) })
      .eq('id', req.params.id);
    if (updateError) throw updateError;

    res.json({ ok: true, km: Number(newKm.toFixed(2)) });
  } catch (error) {
    console.error(error);
    res.status(500).json({ ok: false, error: error.message });
  }
});


app.get('/driver/recap/export.pdf', requireRole('driver'), async (req, res) => {
  try {
    const profile = await fetchProfileById(req.session.user.profile_id);
    if (!profile || profile.driver_type !== 'subcontractor') {
      return res.status(403).send('Export PDF disponible uniquement pour les sous-traitants.');
    }

    const recap = await fetchDriverMonthlyRecap(req.session.user.profile_id, req.query.month);
    buildDriverMonthlyRecapPdf(res, profile, recap);
  } catch (error) {
    console.error(error);
    if (error.code === 'PDFKIT_MISSING') {
      return res.status(500).send(error.message);
    }
    return res.status(500).send('Erreur export PDF récapitulatif');
  }
});

app.get('/client', requireRole('client'), async (req, res) => {
  try {
    const clientProfileId = req.session.user.profile_id;

    const tours = await fetchTours({ client_profile_id: clientProfileId });

    const archivedTours = await fetchTours(
      {
        client_profile_id: clientProfileId,
        is_archived: true
      },
      {
        includeArchived: true
      }
    );

    res.render('client/dashboard', {
      title: 'Suivi client',
      tours,
      archivedTours
    });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur suivi client');
  }
});

app.get('/client/tours/:id', requireRole('client'), async (req, res) => {
  try {
    const tour = await fetchTourById(req.params.id);
    if (!tour || tour.client_id !== req.session.user.profile_id) {
      return res.status(404).send('Tournée introuvable');
    }

    res.render('client/tour', { title: tour.reference, tour });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur détail client');
  }
});

app.get('/api/tours/:id/status', requireAuth, async (req, res) => {
  try {
    const tour = await fetchTourById(req.params.id);
    if (!tour) return res.status(404).json({ ok: false, error: 'Tour introuvable' });

    const isAllowed =
      req.session.user.role === 'dispatch' ||
      (req.session.user.role === 'driver' && tour.driver_id === req.session.user.profile_id) ||
      (req.session.user.role === 'client' && tour.client_id === req.session.user.profile_id);

    if (!isAllowed) return res.status(403).json({ ok: false, error: 'Accès refusé' });

    return res.json({ ok: true, tour });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.get('/media/proofs/:id', requireAuth, async (req, res) => {
  try {
    const { data: proof, error } = await admin
      .from('proof_photos')
      .select('*')
      .eq('id', req.params.id)
      .single();
    if (error) throw error;

    const tour = await fetchTourById(proof.tour_id);

    const isAllowed =
      req.session.user.role === 'dispatch' ||
      (req.session.user.role === 'driver' && tour.driver_id === req.session.user.profile_id) ||
      (req.session.user.role === 'client' && tour.client_id === req.session.user.profile_id);

    if (!isAllowed) return res.status(403).send('Accès refusé');

    const { data: fileData, error: fileError } = await admin.storage
      .from(PROOF_BUCKET)
      .download(proof.file_path);
    if (fileError) throw fileError;

    const arrayBuffer = await fileData.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);

    res.setHeader('Content-Type', fileData.type || 'application/octet-stream');
    res.send(buffer);
  } catch (error) {
    console.error(error);
    res.status(404).send('Preuve introuvable');
  }
});

if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`InterGlobe Web V3 lancé sur http://localhost:${PORT}`);
  });
}

module.exports = app;